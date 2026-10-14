package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlinx.coroutines.test.runTest

class EventTest {
  private val hi = "07000000" + "01000700026869" // event, command 7, data "hi"

  @Test
  fun `an event goes out as one frame with id 0`() = runTest {
    val sent = mutableListOf<String>()
    BareRpc(send = { sent += it.toHexString() }).event(7u, "hi".encodeToByteArray())
    assertEquals(listOf(hi), sent)
  }

  @Test
  fun `a received event reaches the handler`() = runTest {
    val events = mutableListOf<Pair<ULong, String>>()
    BareRpc(send = {}, onEvent = { events += it.command to it.data.decodeToString() })
      .receive(hi.hexToByteArray())
    assertEquals(listOf(7uL to "hi"), events)
  }

  @Test
  fun `an event split across receives arrives once it is whole`() = runTest {
    val events = mutableListOf<ULong>()
    val rpc = BareRpc(send = {}, onEvent = { events += it.command })
    val bytes = hi.hexToByteArray()
    rpc.receive(bytes.copyOfRange(0, 6))
    assertEquals(emptyList(), events)
    rpc.receive(bytes.copyOfRange(6, bytes.size))
    assertEquals(listOf(7uL), events)
  }

  @Test
  fun `a malformed frame fails this receive and every later one`() = runTest {
    val events = mutableListOf<ULong>()
    val rpc = BareRpc(send = {}, onEvent = { events += it.command })
    val failure =
      assertFailsWith<MalformedFrameException> { rpc.receive("020000000100".hexToByteArray()) }
    assertSame(
      failure,
      assertFailsWith<MalformedFrameException> { rpc.receive(hi.hexToByteArray()) },
    )
    assertEquals(emptyList(), events)
  }

  @Test
  fun `a frame over the cap fails receive`() = runTest {
    assertFailsWith<MalformedFrameException> {
      BareRpc(send = {}, maxFrameSize = 8u).receive("05000000".hexToByteArray())
    }
  }
}
