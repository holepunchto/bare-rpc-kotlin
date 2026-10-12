package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertContentEquals
import to.holepunch.compactencoding.encode

class NullPayloadTest {
  @Test
  fun `encodes a null request payload as an empty one`() =
    assertContentEquals(
      "050000000101000000".hexToByteArray(),
      encode(frame, RequestMessage(1uL, 0uL, 0uL, null)),
    )
}
