package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals

class WireFixturesTest {
  @Test
  fun `unsupported names only families the fixtures have`() {
    val families = fixturesDir.listFiles()!!.filter { it.isDirectory }.map { it.name }.toSet()
    assertEquals(emptySet(), unsupported - families)
  }
}
