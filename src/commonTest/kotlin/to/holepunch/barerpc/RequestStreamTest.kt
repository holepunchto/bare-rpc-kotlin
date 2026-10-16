package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

class RequestStreamTest {
  private val open = "0400000001010701" // request id 1, command 7, opens a stream
  private val ack = "050000000301fd0101" // stream id 1, request open
  private val data = "080000000301fd1001026869" // stream id 1, request data "hi"
  private val end = "050000000301fd2001" // stream id 1, request end
  private val pause = "050000000301fd0401" // stream id 1, request pause
  private val resume = "050000000301fd0801" // stream id 1, request resume
  private val ok = "0700000002010000026f6b" // response id 1, "ok"
  private val boom =
    "110000000201010004626f6f6d06455f424f4f4d02" // error id 1: "boom", "E_BOOM", errno 1

  @Test
  fun `a request stream opens and waits for the ack before sending data`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    assertEquals(listOf(open), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `after the ack the body goes out as data then end`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    rpc.receive(ack.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a request stream returns the reply`() = runTest {
    val rpc = BareRpc(send = {})
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    rpc.receive(ack.hexToByteArray())
    rpc.receive(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `an error response before the ack throws and sends no body`() = runTest {
    supervisorScope {
      val sent = mutableListOf<String>()
      val rpc = BareRpc(send = { sent += it.toHexString() })
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      rpc.receive(boom.hexToByteArray())
      assertEquals(listOf(open), sent)
      assertFailsWith<RemoteException> { reply.await() }
    }
  }

  @Test
  fun `close while waiting for the ack throws ConnectionClosedException`() = runTest {
    supervisorScope {
      val rpc = BareRpc(send = {})
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      rpc.close()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `a reply before the ack skips the body`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    rpc.receive(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
    assertEquals(listOf(open), sent)
  }

  @Test
  fun `a pause holds the request body until resume`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    rpc.receive(ack.hexToByteArray())
    rpc.receive(pause.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    rpc.receive(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `close releases a paused request body`() = runTest {
    supervisorScope {
      val rpc = BareRpc(send = {})
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      rpc.receive(ack.hexToByteArray())
      rpc.receive(pause.hexToByteArray())
      yield()
      rpc.close()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `a reply stops a paused request body`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    rpc.receive(ack.hexToByteArray())
    rpc.receive(pause.hexToByteArray())
    yield()
    rpc.receive(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
    assertEquals(listOf(open), sent)
  }
}
