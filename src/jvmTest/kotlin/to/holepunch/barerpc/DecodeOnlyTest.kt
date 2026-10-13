package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import to.holepunch.compactencoding.decode

class DecodeOnlyTest {
  @Test
  fun `every decode-only frame decodes to its message`() {
    for (family in decodeOnlyFamilies) for (case in decodeOnlyCases(family)) assertEquals(
      case.descriptor,
      descriptorOf(decode(frame, case.frame)),
      "$family: ${case.note}",
    )
  }
}
