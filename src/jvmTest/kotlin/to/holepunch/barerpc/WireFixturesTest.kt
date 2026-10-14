package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals

class WireFixturesTest {
  @Test
  fun `every directory is supported`() {
    assertEquals(emptySet(), directories - supported)
  }
}
