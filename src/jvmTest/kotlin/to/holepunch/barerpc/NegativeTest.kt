package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertFailsWith
import to.holepunch.compactencoding.decode

class NegativeTest {
  @Test
  fun `every negative frame is rejected`() {
    for (case in rejections("negative")) assertFailsWith<RpcException>(case.reason) {
      decode(frame, case.frame)
    }
  }
}
