package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
  private val request = "0700000001010700026869" // request id 1, command 7, "hi"
  private val ack = "050000000301fd0102" // stream id 1, response ack
  private val open = "0400000002010001" // response id 1, opens a stream
  private val data = "080000000301fd1002026869" // stream id 1, response data "hi"
  private val end = "050000000301fd2002" // stream id 1, response end
  // stream id 1, response close error: "boom", "E_BOOM", errno 1
  private val closeError = "120000000301fd820204626f6f6d06455f424f4f4d02"
  private val ok = "0700000002010000026f6b" // response id 1, "ok"
  private val requestOpen = "0400000001010701" // request id 1, command 7, opens a stream
  private val requestAck = "050000000301fd0101" // stream id 1, request open
  private val boom =
    "110000000201010004626f6f6d06455f424f4f4d02" // error id 1: "boom", "E_BOOM", errno 1
  private val requestPause = "050000000301fd0401" // stream id 1, request pause

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
}
