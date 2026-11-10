package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import to.holepunch.compactEncoding.decode
import to.holepunch.compactEncoding.encode

@OptIn(ExperimentalCoroutinesApi::class)
class RequestFlowTest {
  private val big =
    "084000000301fd1002fd0040" + "00".repeat(16384) // stream id 1, response data, 16 KiB
  private val pause = "050000000301fd0402" // stream id 1, response pause
  private val resume = "050000000301fd0802" // stream id 1, response resume
  private val request = "0700000001010700026869" // request id 1, command 7, "hi"
  private val ack = "050000000301fd0102" // stream id 1, response ack
  private val open = "0400000002010001" // response id 1, opens a stream
  private val data = "080000000301fd1002026869" // stream id 1, response data "hi"
  private val end = "050000000301fd2002" // stream id 1, response end
  private val destroy = "050000000301fd4002" // stream id 1, response destroy
  private val close = "050000000301fd0202" // stream id 1, response close
  // cancel id 1, CANCELLED
  private val cancel =
    "2300000004011552657175657374207761732063616e63656c6c65640943414e43454c4c454400"
  // stream id 1, response data with an error: "boom", "E_BOOM", errno 1
  private val dataError = "120000000301fd900204626f6f6d06455f424f4f4d02"
  // stream id 1, response close error: "boom", "E_BOOM", errno 1
  private val closeError = "120000000301fd820204626f6f6d06455f424f4f4d02"
  // stream id 1, response end with an error: "boom", "E_BOOM", errno 1
  private val endError = "120000000301fda00204626f6f6d06455f424f4f4d02"
  private val ok = "0700000002010000026f6b" // response id 1, "ok"
  private val requestOpen = "0400000001010701" // request id 1, command 7, opens a stream
  private val requestAck = "050000000301fd0101" // stream id 1, request open
  private val boom =
    "110000000201010004626f6f6d06455f424f4f4d02" // error id 1: "boom", "E_BOOM", errno 1
  private val requestPause = "050000000301fd0401" // stream id 1, request pause
  private val requestResume = "050000000301fd0801" // stream id 1, request resume
  private val requestData = "080000000301fd1001026869" // stream id 1, request data "hi"
  private val requestEnd = "050000000301fd2001" // stream id 1, request end
  private val requestDestroy = "050000000301fd4001" // stream id 1, request destroy
  private val requestClose = "050000000301fd0201" // stream id 1, request close
  // stream id 1, request close error: "boom", "E_BOOM", errno 1
  private val requestCloseError = "120000000301fd820104626f6f6d06455f424f4f4d02"

  @Test
  fun `collecting a request flow sends the request then the ack`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    launch { rpc.requestFlow(7u, "hi".encodeToByteArray()).collect {} }
    yield()
    assertEquals(listOf(request, ack), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a streamed reply is emitted until its end`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    remote.send(open.hexToByteArray())
    remote.send(data.hexToByteArray())
    remote.send(end.hexToByteArray())
    assertEquals(listOf("hi"), chunks.await().map { it.decodeToString() })
  }

  @Test
  fun `a reply flow that ends sends no destroy`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    remote.send(open.hexToByteArray())
    remote.send(end.hexToByteArray())
    chunks.await()
    assertEquals(listOf(request, ack), sent)
  }

  @Test
  fun `a stream closed with an error throws a remote exception`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
      val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
      yield()
      remote.send(open.hexToByteArray())
      remote.send(closeError.hexToByteArray())
      assertEquals("E_BOOM", assertFailsWith<RemoteException> { chunks.await() }.code)
    }
  }

  @Test
  fun `a plain reply is emitted as one chunk`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    remote.send(ok.hexToByteArray())
    assertEquals(listOf("ok"), chunks.await().map { it.decodeToString() })
  }

  @Test
  fun `a streamed reply to request throws UnexpectedStreamException and is destroyed`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    supervisorScope {
      val rpc =
        Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
      val reply = async { rpc.request(7u, "hi".encodeToByteArray()) }
      yield()
      remote.send(open.hexToByteArray())
      assertFailsWith<UnexpectedStreamException> { reply.await() }
      assertEquals(listOf(request, destroy), sent)
      rpc.assertOpen()
    }
  }

  @Test
  fun `a streamed reply to a request with a body closes the body and destroys the reply`() =
    runTest {
      val remote = Remote(this)
      val sent = mutableListOf<Message>()
      supervisorScope {
        val rpc =
          Rpc(
            TestTransport(remote.data, { sent += decode(frame, it) }),
            scope = backgroundScope,
          )
        val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
        yield()
        remote.send(open.hexToByteArray())
        assertFailsWith<UnexpectedStreamException> { reply.await() }
        val frames = sent.drop(1).map { it as Message.Stream }
        assertEquals(
          listOf(STREAM_RESPONSE or STREAM_DESTROY, STREAM_REQUEST or STREAM_CLOSE or STREAM_ERROR),
          frames.map { it.flags },
        )
        assertEquals(
          listOf(null, "UNEXPECTED_STREAM" to 0L),
          frames.map { f -> f.error?.let { it.code to it.errno } },
        )
      }
    }

  @Test
  fun `a reply stream that ends before accepting the body ends the flow`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val chunks = async { rpc.requestFlow(7u, flowOf("hi".encodeToByteArray())).toList() }
    yield()
    remote.send(open.hexToByteArray())
    remote.send(end.hexToByteArray())
    assertEquals(emptyList(), chunks.await())
    assertEquals(listOf(requestOpen, ack, requestClose), sent)
  }

  @Test
  fun `a collector that stops as the reply ends before accepting the body closes it once`() =
    runTest {
      val remote = Remote(this)
      val sent = mutableListOf<String>()
      val rpc =
        Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
      val chunk = async { rpc.requestFlow(7u, flowOf("hi".encodeToByteArray())).first() }
      yield()
      remote.send(open.hexToByteArray())
      // The data wakes the collector first, which stops the body while the end refuses it.
      remote.send((data + end).hexToByteArray())
      assertEquals("hi", chunk.await().decodeToString())
      assertEquals(listOf(requestOpen, ack, requestClose), sent)
    }

  @Test
  fun `reply chunks are emitted while the body is still being sent`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
    val first = async {
      rpc
        .requestFlow(
          7u,
          flow {
            emit("hi".encodeToByteArray())
            awaitCancellation()
          },
        )
        .first()
    }
    yield()
    remote.send(requestAck.hexToByteArray())
    yield()
    remote.send(open.hexToByteArray())
    remote.send(data.hexToByteArray())
    assertEquals("hi", first.await().decodeToString())
  }

  @Test
  fun `an error reply throws a remote exception`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
      val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
      yield()
      remote.send(boom.hexToByteArray())
      assertEquals("E_BOOM", assertFailsWith<RemoteException> { chunks.await() }.code)
    }
  }

  @Test
  fun `a reply that ends after the ack leaves the body streaming to its end`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val chunks = async { rpc.requestFlow(7u, flowOf("hi".encodeToByteArray())).toList() }
    yield()
    remote.send((requestAck + requestPause).hexToByteArray())
    yield()
    remote.send(open.hexToByteArray())
    remote.send(end.hexToByteArray())
    assertEquals(emptyList(), chunks.await())
    assertEquals(listOf(requestOpen, ack), sent)
    remote.send(requestResume.hexToByteArray())
    assertEquals(listOf(requestOpen, ack, requestData, requestEnd), sent)
  }

  @Test
  fun `a reply that reaches the mark pauses the peer`() = runTest {
    val remote = Remote(this)
    val release = CompletableDeferred<Unit>()
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    launch { rpc.requestFlow(7u, "hi".encodeToByteArray()).collect { release.await() } }
    yield()
    remote.send(open.hexToByteArray())
    remote.send(data.hexToByteArray())
    yield()
    remote.send(big.hexToByteArray())
    assertEquals(listOf(request, ack, pause), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a paused reply resumes the peer once drained`() = runTest {
    val remote = Remote(this)
    val release = CompletableDeferred<Unit>()
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    launch { rpc.requestFlow(7u, "hi".encodeToByteArray()).collect { release.await() } }
    yield()
    remote.send(open.hexToByteArray())
    remote.send(data.hexToByteArray())
    yield()
    remote.send(big.hexToByteArray())
    release.complete(Unit)
    yield()
    assertEquals(listOf(request, ack, pause, resume), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a reply stream past the cap fails the connection`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(TestTransport(remote.data), maxBufferedBytes = 1, scope = backgroundScope)
      async { rpc.requestFlow(7u, ByteArray(0)).first() }
      yield()
      remote.send(open.hexToByteArray())
      remote.send(data.hexToByteArray())
      assertFailsWith<StreamOverflowException> { rpc.request(0u, ByteArray(0)) }
    }
  }

  @Test
  fun `a stream ended with an error fails the connection`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
      val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
      yield()
      remote.send(open.hexToByteArray())
      remote.send(data.hexToByteArray())
      remote.send(endError.hexToByteArray())
      assertFailsWith<MalformedFrameException> { chunks.await() }
    }
  }

  @Test
  fun `stopping a reply flow early cancels the request`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val chunk = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).first() }
    yield()
    remote.send(open.hexToByteArray())
    remote.send(data.hexToByteArray())
    assertEquals("hi", chunk.await().decodeToString())
    assertEquals(listOf(request, ack, cancel), sent)
  }

  @Test
  fun `a close after the end is ignored`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    remote.send(open.hexToByteArray())
    remote.send(data.hexToByteArray())
    remote.send(end.hexToByteArray())
    remote.send(close.hexToByteArray())
    assertEquals(listOf("hi"), chunks.await().map { it.decodeToString() })
    assertEquals(listOf(request, ack), sent)
  }

  @Test
  fun `a data frame with an error fails the connection`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
      val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
      yield()
      remote.send(open.hexToByteArray())
      remote.send(data.hexToByteArray())
      remote.send(dataError.hexToByteArray())
      assertFailsWith<MalformedFrameException> { chunks.await() }
    }
  }

  @Test
  fun `a throwing request body fails the flow and closes the stream`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val sent = mutableListOf<String>()
      val rpc =
        Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
      val body = flow<ByteArray> { throw RemoteException("boom", "E_BOOM", 1) }
      val chunks = async { rpc.requestFlow(7u, body).toList() }
      yield()
      remote.send(requestAck.hexToByteArray())
      yield()
      assertEquals("E_BOOM", assertFailsWith<RemoteException> { chunks.await() }.code)
      assertEquals(listOf(requestOpen, ack, requestCloseError, destroy), sent)
    }
  }

  @Test
  fun `a destroy that comes before the request body starts stops it`() = runTest {
    val chunks = Channel<ByteArray>(Channel.UNLIMITED)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(chunks.consumeAsFlow()) {
          sent += it.toHexString()
          if (sent.last() == requestOpen) {
            chunks.trySend((requestAck + requestDestroy).hexToByteArray())
          }
        },
        scope = backgroundScope,
      )
    val reply = async { rpc.requestFlow(7u, flowOf("hi".encodeToByteArray())).toList() }
    runCurrent()
    assertEquals(listOf(requestOpen, ack), sent)
    chunks.trySend((open + end).hexToByteArray())
    assertEquals(emptyList(), reply.await())
  }

  @Test
  fun `an empty plain reply emits no chunk`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    remote.send(Message.Response(1u, 0u, ByteArray(0), null))
    assertEquals(emptyList(), chunks.await())
  }

  @Test
  fun `a paused reply resumes the peer once less than the mark is buffered`() = runTest {
    val remote = Remote(this)
    val permits = Channel<Unit>(Channel.UNLIMITED)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    launch { rpc.requestFlow(7u, "hi".encodeToByteArray()).collect { permits.receive() } }
    yield()
    remote.send(open.hexToByteArray())
    repeat(3) {
      remote.send(Message.Stream(1u, STREAM_RESPONSE or STREAM_DATA, ByteArray(10000), null))
    }
    assertEquals(listOf(request, ack, pause), sent)
    permits.trySend(Unit)
    runCurrent()
    assertEquals(listOf(request, ack, pause, resume), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a failing collector destroys the reply stream with its error`() = runTest {
    val failures =
      listOf(
        RemoteException("boom", "E_BOOM", 1) to RemoteError("boom", "E_BOOM", 1),
        IllegalStateException("broken") to RemoteError("broken", "", 0),
      )
    for ((thrown, error) in failures) {
      val remote = Remote(this)
      supervisorScope {
        val sent = mutableListOf<String>()
        val rpc =
          Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
        val collecting = async {
          rpc.requestFlow(7u, "hi".encodeToByteArray()).collect { throw thrown }
        }
        yield()
        remote.send(open.hexToByteArray())
        remote.send(data.hexToByteArray())
        assertEquals(thrown.message, assertFailsWith<Exception> { collecting.await() }.message)
        val destroyed =
          Message.Stream(1u, STREAM_RESPONSE or STREAM_DESTROY or STREAM_ERROR, null, error)
        assertEquals(listOf(request, ack, encode(frame, destroyed).toHexString()), sent)
      }
    }
  }

  @Test
  fun `a cancelled collector cancels the request`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    val collecting = launch {
      rpc.requestFlow(7u, "hi".encodeToByteArray()).collect { awaitCancellation() }
    }
    yield()
    remote.send(open.hexToByteArray())
    remote.send(data.hexToByteArray())
    collecting.cancel()
    runCurrent()
    assertEquals(listOf(request, ack, cancel), sent)
    remote.send((data + end).hexToByteArray())
    assertEquals(listOf(request, ack, cancel), sent)
    assertEquals(0, rpc.tracked())
    rpc.assertOpen()
  }
}
