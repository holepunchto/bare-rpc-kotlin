package to.holepunch.barerpc.messages

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import to.holepunch.barerpc.MalformedFrameException
import to.holepunch.barerpc.Message
import to.holepunch.barerpc.Messages

class MessagesTest {
  @Test
  fun `a request encodes and decodes through the public codec`() {
    val request =
      Message.Request(id = 1u, command = 42u, stream = 0u, data = "hi".encodeToByteArray())
    val bytes = Messages.encode(request)
    val decoded = Messages.decode(bytes)
    assertEquals(request, decoded.message)
    assertEquals(bytes.size, decoded.read)
  }

  @Test
  fun `a truncated frame is rejected`() {
    assertFailsWith<MalformedFrameException> { Messages.decode(byteArrayOf(4, 0, 0, 0)) }
  }
}
