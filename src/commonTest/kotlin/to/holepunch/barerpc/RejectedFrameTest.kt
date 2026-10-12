package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import to.holepunch.compactencoding.decode

class RejectedFrameTest {
  @Test
  fun `an unknown type is rejected as an unknown message carrying its type`() {
    val error =
      assertFailsWith<UnknownMessageException> { decode(frame, "020000006300".hexToByteArray()) }
    assertEquals(99uL, error.type)
  }
}
