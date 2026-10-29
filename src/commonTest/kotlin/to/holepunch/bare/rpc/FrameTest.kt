package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import to.holepunch.compactEncoding.State

class FrameTest {
  @Test
  fun `decoding a frame leaves the state at the next frame`() {
    val state = State("050000000101000000050000000101000000".hexToByteArray())
    frame.decode(state)
    assertEquals(9, state.start)
    assertEquals(18, state.end)
  }

  @Test
  fun `bytes a frame's fields leave unread are skipped`() {
    val state = State("060000000302fd200100".hexToByteArray())
    frame.decode(state)
    assertEquals(10, state.start)
  }
}
