package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import to.holepunch.compactEncoding.encode

class IncomingRequestStreamTest {
  private val big =
    "084000000301fd1001fd0040" + "00".repeat(16384) // stream id 1, request data, 16 KiB
  private val pause = "050000000301fd0401" // stream id 1, request pause
  private val resume = "050000000301fd0801" // stream id 1, request resume
  private val open = "0400000001010701" // request id 1, command 7, opens a stream
  private val ack = "050000000301fd0101" // stream id 1, request open
  private val data = "080000000301fd1001026869" // stream id 1, request data "hi"
  private val end = "050000000301fd2001" // stream id 1, request end
  private val reply = "0700000002010000026869" // response id 1, "hi"
  private val close = "050000000301fd0201" // stream id 1, request close
  private val destroy = "050000000301fd4001" // stream id 1, request destroy
  private val replyOpen = "0400000002010001" // response id 1, opens a stream
  private val replyAck = "050000000301fd0102" // stream id 1, response open
  private val replyData = "080000000301fd1002026869" // stream id 1, response data "hi"
  private val replyEnd = "050000000301fd2002" // stream id 1, response end
  // stream id 1, request data with an error: "boom", "E_BOOM", errno 1
  private val dataError = "120000000301fd900104626f6f6d06455f424f4f4d02"
  // stream id 1, request close error: "boom", "E_BOOM", errno 1
  private val closeError = "120000000301fd820104626f6f6d06455f424f4f4d02"
  private val boom =
    "110000000201010004626f6f6d06455f424f4f4d02" // error id 1: "boom", "E_BOOM", errno 1
  // stream id 1, request end with an error: "boom", "E_BOOM", errno 1
  private val endError = "120000000301fda00104626f6f6d06455f424f4f4d02"
  private val empty = "050000000201000000" // response id 1, empty

  @Test
  fun `collecting a request stream sends the ack`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = {
          it.body!!.collect {}
          Reply.Data(ByteArray(0))
        },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    assertEquals(listOf(ack), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a request stream's data reaches the handler and its reply goes back`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    remote.send(end.hexToByteArray())
    yield()
    assertEquals(listOf(ack, reply), sent)
  }

  @Test
  fun `a close without an error ends the body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    remote.send(close.hexToByteArray())
    yield()
    assertEquals(listOf(ack, reply), sent)
  }

  @Test
  fun `close ends a request body with the cause`() = runTest {
    val remote = Remote(this)
    val caught = CompletableDeferred<Throwable>()
    val rpc =
      Rpc(
        TestTransport(remote.data),
        onRequest = {
          try {
            it.body!!.collect {}
          } catch (e: Throwable) {
            caught.complete(e)
          }
          Reply.Data(ByteArray(0))
        },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    rpc.close()
    assertIs<ConnectionClosedException>(caught.await())
  }

  @Test
  fun `a stream request has empty data`() = runTest {
    val remote = Remote(this)
    val seen = CompletableDeferred<Int>()
    val rpc =
      Rpc(
        TestTransport(remote.data),
        onRequest = {
          seen.complete(it.data.size)
          it.body!!.collect {}
          Reply.Data(ByteArray(0))
        },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    assertEquals(seen.await(), 0)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a body that reaches the mark pauses the peer`() = runTest {
    val remote = Remote(this)
    val release = CompletableDeferred<Unit>()
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = {
          it.body!!.collect { release.await() }
          Reply.Data(ByteArray(0))
        },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    yield()
    remote.send(big.hexToByteArray())
    assertEquals(listOf(ack, pause), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a paused body resumes the peer once drained`() = runTest {
    val remote = Remote(this)
    val release = CompletableDeferred<Unit>()
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = {
          it.body!!.collect { release.await() }
          Reply.Data(ByteArray(0))
        },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    yield()
    remote.send(big.hexToByteArray())
    release.complete(Unit)
    yield()
    assertEquals(listOf(ack, pause, resume), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a failure cancels a handler still reading a streamed body`() = runTest {
    val remote = Remote(this)
    val stopped = CompletableDeferred<Unit>()
    Rpc(
      TestTransport(remote.data),
      onRequest = {
        try {
          it.body!!.collect { awaitCancellation() }
        } finally {
          stopped.complete(Unit)
        }
        Reply.Data(ByteArray(0))
      },
      scope = backgroundScope,
    )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    remote.send("020000006300".hexToByteArray())
    assertTrue(stopped.isCompleted)
  }

  @Test
  fun `close cancels a handler still reading a streamed body`() = runTest {
    val remote = Remote(this)
    val stopped = CompletableDeferred<Unit>()
    var taken = 0
    val rpc =
      Rpc(
        TestTransport(remote.data),
        onRequest = {
          try {
            it.body!!.collect {
              taken++
              awaitCancellation()
            }
          } finally {
            stopped.complete(Unit)
          }
          Reply.Data(ByteArray(0))
        },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    remote.send(big.hexToByteArray())
    rpc.close()
    assertTrue(stopped.isCompleted)
    assertEquals(1, taken)
  }

  @Test
  fun `a request stream past the cap fails the connection`() = runTest {
    val remote = Remote(this)
    val rpc =
      Rpc(
        TestTransport(remote.data),
        onRequest = { awaitCancellation() },
        maxBufferedBytes = 1,
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    assertFailsWith<StreamOverflowException> { rpc.request(0u, ByteArray(0)) }
    remote.send(end.hexToByteArray())
    assertFailsWith<StreamOverflowException> { rpc.request(0u, ByteArray(0)) }
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a request stream up to the cap is kept`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        maxBufferedBytes = 2,
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    remote.send(end.hexToByteArray())
    yield()
    assertEquals(listOf(ack, reply), sent)
  }

  @Test
  fun `a close with an error fails the body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(closeError.hexToByteArray())
    yield()
    assertEquals(listOf(ack, boom), sent)
  }

  @Test
  fun `an end with an error fails the connection`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(endError.hexToByteArray())
    assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) }
    assertEquals(listOf(ack), sent)
  }

  @Test
  fun `a handler that stops reading early sends a destroy`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(it.body!!.first()) },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    yield()
    assertEquals(listOf(ack, destroy, reply), sent)
  }

  @Test
  fun `a handler that ignores the body opens and destroys it`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(ByteArray(0)) },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    assertEquals(listOf(empty, ack, destroy), sent)
  }

  @Test
  fun `a close after the end is ignored`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    remote.send(end.hexToByteArray())
    remote.send(close.hexToByteArray())
    yield()
    assertEquals(listOf(ack, reply), sent)
  }

  @Test
  fun `a cancelled scope closes the connection`() = runTest {
    val remote = Remote(this)
    val rpc =
      Rpc(
        TestTransport(remote.data),
        scope = CoroutineScope(coroutineContext + Job().apply { cancel() }),
      )
    yield()
    assertFailsWith<ConnectionClosedException> { rpc.request(7u, ByteArray(0)) }
  }

  @Test
  fun `a data frame with an error fails the connection`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(dataError.hexToByteArray())
    assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) }
    assertEquals(listOf(ack), sent)
  }

  @Test
  fun `a body streamed back as the reply is read to its end`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(it.body!!) },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(replyAck.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    remote.send(end.hexToByteArray())
    yield()
    assertEquals(listOf(replyOpen, ack, replyData, replyEnd), sent)
  }

  @Test
  fun `a body read after the handler returns ends empty`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val late = CompletableDeferred<List<ByteArray>>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { request ->
          launch { late.complete(request.body!!.toList()) }
          Reply.Data(ByteArray(0))
        },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    assertEquals(emptyList(), late.await())
    assertEquals(listOf(empty, ack, destroy), sent)
  }

  @Test
  fun `a failing body collector destroys the request stream with its error`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { request ->
          request.body!!.collect { throw RemoteException("boom", "E_BOOM", 1) }
          Reply.Data(ByteArray(0))
        },
        scope = backgroundScope,
      )
    remote.send(open.hexToByteArray())
    yield()
    remote.send(data.hexToByteArray())
    val error = RemoteError("boom", "E_BOOM", 1)
    val destroyed =
      Message.Stream(1u, STREAM_REQUEST or STREAM_DESTROY or STREAM_ERROR, null, error)
    assertEquals(listOf(ack, encode(frame, destroyed).toHexString(), boom), sent)
  }

  @Test
  fun `collecting a body a second time throws and leaves the first collection be`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    Rpc(
      TestTransport(remote.data, { sent += it.toHexString() }),
      onRequest = { request ->
        val body = request.body!!
        val first = async { body.fold(ByteArray(0)) { a, b -> a + b } }
        yield()
        assertFailsWith<AlreadyReceivedException> { body.toList() }
        Reply.Data(first.await())
      },
      scope = backgroundScope,
    )
    remote.send(open.hexToByteArray())
    remote.send(data.hexToByteArray())
    remote.send(end.hexToByteArray())
    assertEquals(listOf(ack, reply), sent)
  }

  @Test
  fun `collecting a body again after its end throws`() = runTest {
    val remote = Remote(this)
    val second = CompletableDeferred<Throwable?>()
    Rpc(
      TestTransport(remote.data),
      onRequest = { request ->
        request.body!!.toList()
        second.complete(runCatching { request.body.toList() }.exceptionOrNull())
        Reply.Data(ByteArray(0))
      },
      scope = backgroundScope,
    )
    remote.send(open.hexToByteArray())
    remote.send(end.hexToByteArray())
    assertIs<AlreadyReceivedException>(second.await())
  }
}
