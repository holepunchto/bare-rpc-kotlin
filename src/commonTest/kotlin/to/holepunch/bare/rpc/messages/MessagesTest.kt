package to.holepunch.bare.rpc.messages

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import to.holepunch.bare.rpc.MalformedFrameException
import to.holepunch.bare.rpc.Message
import to.holepunch.bare.rpc.Messages
import to.holepunch.bare.rpc.RemoteError

class MessagesTest {
  @Test
  fun `a request encodes and decodes through the public codec`() {
    val request =
      Message.Request(id = 1u, command = 42u, stream = 0u, data = "hi".encodeToByteArray())
    val bytes = Messages.encode(request)
    val decoded = Messages.decode(bytes)
    assertEquals(request, decoded.message)
    assertEquals(bytes.size, decoded.bytesRead)
  }

  @Test
  fun `a cancel has the bytes of librpc`() {
    val cancel = Message.Cancel(1u, RemoteError("Cancelled", "CANCELLED", -1))
    val bytes = "17000000040109${"Cancelled".hex()}09${"CANCELLED".hex()}01".hexToByteArray()
    assertContentEquals(bytes, Messages.encode(cancel))
    assertEquals(cancel, Messages.decode(bytes).message)
  }

  @Test
  fun `a truncated frame is rejected`() {
    assertFailsWith<MalformedFrameException> { Messages.decode(byteArrayOf(4, 0, 0, 0)) }
  }
}

private fun String.hex() = encodeToByteArray().toHexString()
