package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals

class WireFixturesTest {
  @Test
  fun `unsupported names only directories the fixtures have`() {
    val directories = fixtures.listFiles()!!.filter { it.isDirectory }.map { it.name }.toSet()
    assertEquals(emptySet(), unsupported - directories)
  }

  @Test
  fun `every directory is supported or unsupported`() {
    val directories = fixtures.listFiles()!!.filter { it.isDirectory }.map { it.name }.toSet()
    assertEquals(emptySet(), directories - supported - unsupported)
  }
}
