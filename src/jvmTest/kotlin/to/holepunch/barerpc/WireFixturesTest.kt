package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals

class WireFixturesTest {
  @Test
  fun `unsupported names only directories the fixtures have`() {
    assertEquals(emptySet(), unsupported - directories)
  }

  @Test
  fun `every directory is supported or unsupported`() {
    assertEquals(emptySet(), directories - supported - unsupported)
  }
}
