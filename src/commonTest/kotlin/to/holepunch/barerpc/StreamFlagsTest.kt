package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import to.holepunch.compactencoding.decode
import to.holepunch.compactencoding.encode

class StreamFlagsTest {
  private val bothFlags = "0d0000000302fd920104676f6e65014502".hexToByteArray()

  @Test
  fun `a stream error with the data flag encodes only the error`() =
    assertContentEquals(
      bothFlags,
      encode(frame, StreamMessage(2uL, 0x192uL, byteArrayOf(1), RemoteError("gone", "E", 1))),
    )

  @Test
  fun `a stream frame with both flags decodes as an error`() {
    val message = decode(frame, bothFlags) as StreamMessage
    assertNull(message.data)
    assertEquals("gone", message.error?.message)
  }

  @Test
  fun `a stream error without the error flag encodes the data`() =
    assertContentEquals(
      "070000000302fd10010101".hexToByteArray(),
      encode(frame, StreamMessage(2uL, 0x110uL, byteArrayOf(1), RemoteError("gone", "E", 1))),
    )
}
