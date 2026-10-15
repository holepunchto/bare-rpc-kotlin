package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

class CloseTest {
  @Test
  fun `close resumes a pending request with a connection closed exception`() = runTest {
    supervisorScope {
      val rpc = BareRpc(send = {})
      val reply = async { rpc.request(7u, ByteArray(0)) }
      yield()
      rpc.close()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `every call after close throws`() = runTest {
    val rpc = BareRpc(send = {})
    rpc.close()
    assertFailsWith<ConnectionClosedException> { rpc.request(7u, ByteArray(0)) }
    assertFailsWith<ConnectionClosedException> { rpc.event(7u, ByteArray(0)) }
    assertFailsWith<ConnectionClosedException> {
      rpc.receive("0700000001000700026869".hexToByteArray())
    }
  }

  @Test
  fun `close after a failure keeps the failure`() = runTest {
    val rpc = BareRpc(send = {})
    assertFailsWith<MalformedFrameException> { rpc.receive("020000000100".hexToByteArray()) }
    rpc.close()
    assertFailsWith<MalformedFrameException> { rpc.request(7u, ByteArray(0)) }
  }

  @Test
  fun `close stops the rest of the chunk`() = runTest {
    val events = mutableListOf<ULong>()
    lateinit var rpc: BareRpc
    rpc =
      BareRpc(
        send = {},
        onEvent = {
          events += it.command
          rpc.close()
        },
      )
    val two = "0700000001000700026869" + "0700000001000800026869"
    assertFailsWith<ConnectionClosedException> { rpc.receive(two.hexToByteArray()) }
    assertEquals(listOf(7uL), events)
  }
}
