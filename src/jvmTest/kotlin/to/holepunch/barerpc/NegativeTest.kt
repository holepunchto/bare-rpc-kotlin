package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertFailsWith
import to.holepunch.compactencoding.decode

class NegativeTest {
  @Test
  fun `every negative frame is rejected`() {
    for (family in rejectionFamilies) for (case in rejections(family)) {
      assertFailsWith<RpcException>("$family: ${case.reason}") { decode(frame, case.frame) }
    }
  }
}
