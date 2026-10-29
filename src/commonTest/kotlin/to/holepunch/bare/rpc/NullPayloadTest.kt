package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertContentEquals
import to.holepunch.compactEncoding.encode

class NullPayloadTest {
  @Test
  fun `encodes a null request payload as an empty one`() =
    assertContentEquals(
      "050000000101000000".hexToByteArray(),
      encode(frame, Message.Request(1uL, 0uL, 0uL, null)),
    )

  @Test
  fun `encodes a null response payload as an empty one`() =
    assertContentEquals(
      "050000000201000000".hexToByteArray(),
      encode(frame, Message.Response(1uL, 0uL, null, null)),
    )
}
