package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

class RequestTest {
  private val first = "0700000001010700026869" // request id 1, command 7, "hi"
  private val second = "0700000001020700026869" // request id 2
  private val ok = "0700000002010000026f6b" // response id 1, "ok"
  private val boom =
    "110000000201010004626f6f6d06455f424f4f4d02" // error id 1: "boom", "E_BOOM", errno 1

  @Test
  fun `each request goes out as one frame with the next id`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    launch { rpc.request(7u, "hi".encodeToByteArray()) }
    launch { rpc.request(7u, "hi".encodeToByteArray()) }
    yield()
    assertEquals(listOf(first, second), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a response resumes its request with the data`() = runTest {
    val rpc = BareRpc(send = {})
    val reply = async { rpc.request(7u, "hi".encodeToByteArray()) }
    yield()
    rpc.receive(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `an error response throws a remote exception`() = runTest {
    supervisorScope {
      val rpc = BareRpc(send = {})
      val reply = async { rpc.request(7u, "hi".encodeToByteArray()) }
      yield()
      rpc.receive(boom.hexToByteArray())
      val error = assertFailsWith<RemoteException> { reply.await() }
      assertEquals(Triple("boom", "E_BOOM", 1L), Triple(error.message, error.code, error.errno))
    }
  }

  @Test
  fun `a failure resumes pending requests with it`() = runTest {
    supervisorScope {
      val rpc = BareRpc(send = {})
      val reply = async { rpc.request(7u, "hi".encodeToByteArray()) }
      yield()
      val failure =
        assertFailsWith<MalformedFrameException> { rpc.receive("020000000100".hexToByteArray()) }
      val error = assertFailsWith<MalformedFrameException> { reply.await() }
      assertTrue(error === failure || error.cause === failure)
    }
  }

  @Test
  fun `a request after a failure throws it and sends nothing`() = runTest {
    val sent = mutableListOf<ByteArray>()
    val rpc = BareRpc(send = { sent += it })
    val failure =
      assertFailsWith<MalformedFrameException> { rpc.receive("020000000100".hexToByteArray()) }
    assertSame(failure, assertFailsWith<MalformedFrameException> { rpc.request(7u, ByteArray(0)) })
    assertEquals(emptyList(), sent)
  }

  @Test
  fun `a response with nothing pending is ignored`() = runTest {
    BareRpc(send = {}).receive(ok.hexToByteArray())
  }

  @Test
  fun `concurrent requests send whole frames one at a time`() = runTest {
    val log = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = {
          log += "start"
          yield()
          log += "end"
        }
      )
    launch { rpc.request(7u, ByteArray(0)) }
    launch { rpc.request(7u, ByteArray(0)) }
    delay(1)
    assertEquals(listOf("start", "end", "start", "end"), log)
    coroutineContext.cancelChildren()
  }
}
