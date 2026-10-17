package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

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
  // stream id 1, request close error: "boom", "E_BOOM", errno 1
  private val requestCloseError = "120000000301fd820104626f6f6d06455f424f4f4d02"

  @Test
  fun `collecting a request flow sends the request then the ack`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    launch { rpc.requestFlow(7u, "hi".encodeToByteArray()).collect {} }
    yield()
    assertEquals(listOf(request, ack), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a streamed reply is emitted until its end`() = runTest {
    val rpc = BareRpc(send = {})
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(data.hexToByteArray())
    rpc.receive(end.hexToByteArray())
    assertEquals(listOf("hi"), chunks.await().map { it.decodeToString() })
  }

  @Test
  fun `a reply flow that ends sends no destroy`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(end.hexToByteArray())
    chunks.await()
    assertEquals(listOf(request, ack), sent)
  }

  @Test
  fun `a stream closed with an error throws a remote exception`() = runTest {
    supervisorScope {
      val rpc = BareRpc(send = {})
      val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
      yield()
      rpc.receive(open.hexToByteArray())
      rpc.receive(closeError.hexToByteArray())
      assertEquals("E_BOOM", assertFailsWith<RemoteException> { chunks.await() }.code)
    }
  }

  @Test
  fun `a plain reply is emitted as one chunk`() = runTest {
    val rpc = BareRpc(send = {})
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    rpc.receive(ok.hexToByteArray())
    assertEquals(listOf("ok"), chunks.await().map { it.decodeToString() })
  }

  @Test
  fun `a streamed reply to request throws UnexpectedStreamException`() = runTest {
    supervisorScope {
      val rpc = BareRpc(send = {})
      val reply = async { rpc.request(7u, "hi".encodeToByteArray()) }
      yield()
      rpc.receive(open.hexToByteArray())
      assertFailsWith<UnexpectedStreamException> { reply.await() }
    }
  }

  @Test
  fun `a reply stream that ends before accepting the body ends the flow`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    val chunks = async { rpc.requestFlow(7u, flowOf("hi".encodeToByteArray())).toList() }
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(end.hexToByteArray())
    assertEquals(emptyList(), chunks.await())
    assertEquals(listOf(requestOpen, ack), sent)
  }

  @Test
  fun `reply chunks are emitted while the body is still being sent`() = runTest {
    val rpc = BareRpc(send = {})
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
    rpc.receive(requestAck.hexToByteArray())
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(data.hexToByteArray())
    assertEquals("hi", first.await().decodeToString())
  }

  @Test
  fun `an error reply throws a remote exception`() = runTest {
    supervisorScope {
      val rpc = BareRpc(send = {})
      val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
      yield()
      rpc.receive(boom.hexToByteArray())
      assertEquals("E_BOOM", assertFailsWith<RemoteException> { chunks.await() }.code)
    }
  }

  @Test
  fun `a reply that ends stops a paused request body`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    val chunks = async { rpc.requestFlow(7u, flowOf("hi".encodeToByteArray())).toList() }
    yield()
    rpc.receive(requestAck.hexToByteArray())
    rpc.receive(requestPause.hexToByteArray())
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(end.hexToByteArray())
    assertEquals(emptyList(), chunks.await())
    assertEquals(listOf(requestOpen, ack), sent)
  }

  @Test
  fun `a reply that reaches the mark pauses the peer`() = runTest {
    val release = CompletableDeferred<Unit>()
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    launch { rpc.requestFlow(7u, "hi".encodeToByteArray()).collect { release.await() } }
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(data.hexToByteArray())
    yield()
    rpc.receive(big.hexToByteArray())
    assertEquals(listOf(request, ack, pause), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a paused reply resumes the peer once drained`() = runTest {
    val release = CompletableDeferred<Unit>()
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    launch { rpc.requestFlow(7u, "hi".encodeToByteArray()).collect { release.await() } }
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(data.hexToByteArray())
    yield()
    rpc.receive(big.hexToByteArray())
    release.complete(Unit)
    yield()
    assertEquals(listOf(request, ack, pause, resume), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a reply stream past the cap fails the connection`() = runTest {
    supervisorScope {
      val rpc = BareRpc(send = {}, maxBufferedBytes = 1u)
      async { rpc.requestFlow(7u, ByteArray(0)).first() }
      yield()
      rpc.receive(open.hexToByteArray())
      assertFailsWith<StreamOverflowException> { rpc.receive(data.hexToByteArray()) }
    }
  }

  @Test
  fun `a stream ended with an error ends the flow`() = runTest {
    val rpc = BareRpc(send = {})
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(data.hexToByteArray())
    rpc.receive(endError.hexToByteArray())
    assertEquals(listOf("hi"), chunks.await().map { it.decodeToString() })
  }

  @Test
  fun `stopping a reply flow early sends a destroy`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    val chunk = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).first() }
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(data.hexToByteArray())
    assertEquals("hi", chunk.await().decodeToString())
    assertEquals(listOf(request, ack, destroy), sent)
  }

  @Test
  fun `a close after the end is ignored`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(data.hexToByteArray())
    rpc.receive(end.hexToByteArray())
    rpc.receive(close.hexToByteArray())
    assertEquals(listOf("hi"), chunks.await().map { it.decodeToString() })
    assertEquals(listOf(request, ack), sent)
  }

  @Test
  fun `a data frame without data ends the flow cleanly`() = runTest {
    val rpc = BareRpc(send = {})
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    rpc.receive(open.hexToByteArray())
    rpc.receive(data.hexToByteArray())
    rpc.receive(dataError.hexToByteArray())
    assertEquals(listOf("hi"), chunks.await().map { it.decodeToString() })
  }

  @Test
  fun `a throwing request body fails the flow and closes the stream`() = runTest {
    supervisorScope {
      val sent = mutableListOf<String>()
      val rpc = BareRpc(send = { sent += it.toHexString() })
      val body = flow<ByteArray> { throw RemoteException("boom", "E_BOOM", 1) }
      val chunks = async { rpc.requestFlow(7u, body).toList() }
      yield()
      rpc.receive(requestAck.hexToByteArray())
      yield()
      assertEquals("E_BOOM", assertFailsWith<RemoteException> { chunks.await() }.code)
      assertEquals(listOf(requestOpen, ack, requestCloseError, destroy), sent)
    }
  }
}
