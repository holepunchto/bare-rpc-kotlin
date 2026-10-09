package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import to.holepunch.compactencoding.decode
import to.holepunch.compactencoding.encode

class EnvelopeTest {
  @Test
  fun `every envelope message encodes to its frame`() {
    for (case in cases("envelope")) assertContentEquals(
      case.frame,
      encode(frame, messageOf(case.descriptor)),
      case.note,
    )
  }

  @Test
  fun `every envelope frame decodes to its message`() {
    for (case in cases("envelope")) assertEquals(
      case.descriptor,
      descriptorOf(decode(frame, case.frame)),
      case.note,
    )
  }
}
