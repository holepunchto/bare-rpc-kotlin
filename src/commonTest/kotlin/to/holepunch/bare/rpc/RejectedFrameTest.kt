package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import to.holepunch.compactEncoding.decode

class RejectedFrameTest {
  @Test
  fun `an unknown type is rejected as an unknown message carrying its type`() {
    val error =
      assertFailsWith<UnknownMessageException> { decode(frame, "020000006300".hexToByteArray()) }
    assertEquals(99uL, error.type)
  }

  @Test
  fun `a field past the frame length is malformed even when bytes follow`() {
    assertFailsWith<MalformedFrameException> {
      decode(frame, "020000000107000000".hexToByteArray())
    }
  }

  @Test
  fun `a type with no id after it is malformed`() {
    assertFailsWith<MalformedFrameException> { decode(frame, "0100000063".hexToByteArray()) }
  }
}
