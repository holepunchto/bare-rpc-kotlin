package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

@OptIn(ExperimentalCoroutinesApi::class)
class RequestStreamTest {
  private val open = "0400000001010701" // request id 1, command 7, opens a stream
  private val ack = "050000000301fd0101" // stream id 1, request open
  private val data = "080000000301fd1001026869" // stream id 1, request data "hi"
  private val end = "050000000301fd2001" // stream id 1, request end
  private val close = "050000000301fd0201" // stream id 1, request close
  private val pause = "050000000301fd0401" // stream id 1, request pause
  private val resume = "050000000301fd0801" // stream id 1, request resume
  // stream id 1, request close error: "boom", "E_BOOM", errno 1
  private val closeError = "120000000301fd820104626f6f6d06455f424f4f4d02"
  private val destroy = "050000000301fd4001" // stream id 1, request destroy
  private val cancel =
    "2300000004011552657175657374207761732063616e63656c6c65640943414e43454c4c454400" // cancel id 1,
  // CANCELLED
  // stream id 1, request destroy error: "boom", "E_BOOM", errno 1
  private val destroyError = "120000000301fdc00104626f6f6d06455f424f4f4d02"
  private val ok = "0700000002010000026f6b" // response id 1, "ok"
  private val boom =
    "110000000201010004626f6f6d06455f424f4f4d02" // error id 1: "boom", "E_BOOM", errno 1

  @Test
  fun `a request stream opens and waits for the ack before sending data`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    assertEquals(listOf(open), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `after the ack the body goes out as data then end`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send(ack.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a request stream returns the reply`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send(ack.hexToByteArray())
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `an error response before the ack closes the request stream with the error`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val sent = mutableListOf<String>()
      val rpc =
        Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      remote.send(boom.hexToByteArray())
      assertEquals(listOf(open, closeError), sent)
      assertFailsWith<RemoteException> { reply.await() }
    }
  }

  @Test
  fun `close while waiting for the ack throws ConnectionClosedException`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      rpc.close()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `a reply before the ack skips the body and closes the request stream`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
    assertEquals(listOf(open, close), sent)
  }

  @Test
  fun `a pause holds the request body until resume`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send((ack + pause).hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    remote.send(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `close releases a paused request body`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      remote.send((ack + pause).hexToByteArray())
      yield()
      rpc.close()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `a reply after the ack returns while the body streams to its end`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send((ack + pause).hexToByteArray())
    yield()
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
    assertEquals(listOf(open), sent)
    remote.send(resume.hexToByteArray())
    assertEquals(listOf(open, data, end), sent)
  }

  @Test
  fun `a destroy stops the request body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send((ack + pause).hexToByteArray())
    yield()
    remote.send(destroy.hexToByteArray())
    remote.send(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `a destroy with an error stops the request body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send((ack + pause).hexToByteArray())
    yield()
    remote.send(destroyError.hexToByteArray())
    remote.send(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `a throwing request body closes the stream with the error`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val sent = mutableListOf<String>()
      val rpc =
        Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
      val body = flow<ByteArray> { throw RemoteException("boom", "E_BOOM", 1) }
      val reply = async { rpc.request(7u, body) }
      yield()
      remote.send(ack.hexToByteArray())
      yield()
      assertEquals("E_BOOM", assertFailsWith<RemoteException> { reply.await() }.code)
      assertEquals(listOf(open, closeError), sent)
    }
  }

  @Test
  fun `a destroy that comes before the body starts stops it`() = runTest {
    val chunks = Channel<ByteArray>(Channel.UNLIMITED)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(
          chunks.consumeAsFlow(),
          {
            sent += it.toHexString()
            if (sent.last() == open) chunks.trySend((ack + destroy).hexToByteArray())
          },
        ),
        scope = backgroundScope,
      )
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    runCurrent()
    assertEquals(listOf(open), sent)
    chunks.trySend(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `an error reply after the ack leaves the body streaming to its end`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val sent = mutableListOf<String>()
      val rpc =
        Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      remote.send((ack + pause).hexToByteArray())
      remote.send(boom.hexToByteArray())
      assertEquals("E_BOOM", assertFailsWith<RemoteException> { reply.await() }.code)
      remote.send(resume.hexToByteArray())
      assertEquals(listOf(open, data, end), sent)
    }
  }

  @Test
  fun `close stops a body that is still streaming after the reply`() = runTest {
    val remote = Remote(this)
    val stopped = CompletableDeferred<Unit>()
    val body = flow {
      emit("hi".encodeToByteArray())
      try {
        awaitCancellation()
      } finally {
        stopped.complete(Unit)
      }
    }
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val reply = async { rpc.request(7u, body) }
    yield()
    remote.send(ack.hexToByteArray())
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
    assertEquals(listOf(open, data), sent)
    rpc.close()
    runCurrent()
    assertTrue(stopped.isCompleted)
  }

  @Test
  fun `cancelling the call before the reply stops the body`() = runTest {
    val remote = Remote(this)
    val stopped = CompletableDeferred<Unit>()
    val body = flow {
      emit("hi".encodeToByteArray())
      try {
        awaitCancellation()
      } finally {
        stopped.complete(Unit)
      }
    }
    val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
    val call = launch { rpc.request(7u, body) }
    yield()
    remote.send(ack.hexToByteArray())
    call.cancel()
    runCurrent()
    assertTrue(stopped.isCompleted)
  }

  @Test
  fun `cancelling a request whose body streams cancels it without closing the stream`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val body = flow {
      emit("hi".encodeToByteArray())
      awaitCancellation()
    }
    val reply = launch { rpc.request(7u, body) }
    yield()
    remote.send(ack.hexToByteArray())
    reply.cancel()
    runCurrent()
    assertEquals(listOf(open, data, cancel), sent)
  }

  @Test
  fun `cancelling a request before the peer accepts its body sends no body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val reply = launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    reply.cancel()
    runCurrent()
    assertEquals(listOf(open, cancel), sent)
    remote.send(ack.hexToByteArray())
    assertEquals(listOf(open, cancel), sent)
  }

  @Test
  fun `a body the peer destroys is not closed when its request is cancelled`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val reply = launch { rpc.request(7u, flow { awaitCancellation() }) }
    yield()
    remote.send((ack + destroy).hexToByteArray())
    reply.cancel()
    runCurrent()
    assertEquals(listOf(open, cancel), sent)
  }
}
