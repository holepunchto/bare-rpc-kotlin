package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals

class InteropTest {
  @Test
  fun `a request to the JS peer comes back`() = withPeer { rpc ->
    assertEquals("hi", rpc.request(0u, "hi".encodeToByteArray()).decodeToString())
  }
}
