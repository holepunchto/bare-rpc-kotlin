package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive
import to.holepunch.compactencoding.decode
import to.holepunch.compactencoding.encode

class ErrorTest {
  private val responses = cases("error").filter { it.descriptor["type"]!!.jsonPrimitive.int == 2 }

  @Test
  fun `every error response encodes to its frame`() {
    for (case in responses) assertContentEquals(
      case.frame,
      encode(frame, messageOf(case.descriptor)),
      case.note,
    )
  }

  @Test
  fun `every error response frame decodes to its message`() {
    for (case in responses) assertEquals(
      case.descriptor,
      descriptorOf(decode(frame, case.frame)),
      case.note,
    )
  }
}
