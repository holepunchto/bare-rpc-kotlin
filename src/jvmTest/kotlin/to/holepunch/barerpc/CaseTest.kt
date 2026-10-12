package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import to.holepunch.compactencoding.decode
import to.holepunch.compactencoding.encode

class CaseTest {
  @Test
  fun `every message encodes to its frame`() {
    for (family in caseFamilies) for (case in cases(family)) assertContentEquals(
      case.frame,
      encode(frame, messageOf(case.descriptor)),
      "$family: ${case.note}",
    )
  }

  @Test
  fun `every frame decodes to its message`() {
    for (family in caseFamilies) for (case in cases(family)) assertEquals(
      case.descriptor,
      descriptorOf(decode(frame, case.frame)),
      "$family: ${case.note}",
    )
  }
}
