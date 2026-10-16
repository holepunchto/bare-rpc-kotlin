package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlinx.coroutines.test.runTest
import to.holepunch.compactencoding.decode
import to.holepunch.compactencoding.encode

class StreamFlagsTest {
  private val bothFlags = "0d0000000302fd920104676f6e65014502".hexToByteArray()
  private val noDirection = "03000000030104" // stream id 1, pause, neither request nor response
  private val hi = "0700000001050700026869" // request id 5, command 7, "hi"
  private val unknown =
    "2b0000000205010011556e6b6e6f776e20636f6d6d616e642037134552525f554e4b4e4f574e5f434f4d4d414e4400"

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
  fun `a stream message with an error but only the data flag encodes the data`() =
    assertContentEquals(
      "070000000302fd10010101".hexToByteArray(),
      encode(frame, StreamMessage(2uL, 0x110uL, byteArrayOf(1), RemoteError("gone", "E", 1))),
    )

  @Test
  fun `a stream frame without a direction is ignored`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    rpc.receive(noDirection.hexToByteArray())
    rpc.receive(hi.hexToByteArray())
    assertEquals(listOf(unknown), sent)
  }
}
