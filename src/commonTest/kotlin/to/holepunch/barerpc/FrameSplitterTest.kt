package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals

class FrameSplitterTest {
  private val first = "050000000101000000".hexToByteArray()
  private val second = "050000000102000000".hexToByteArray()

  private fun FrameSplitter.frames(bytes: ByteArray) = push(bytes).map { it.toList() }

  @Test
  fun `a whole frame comes out as it went in`() {
    assertEquals(listOf(first.toList()), FrameSplitter().frames(first))
  }

  @Test
  fun `two frames in one chunk come out as two`() {
    assertEquals(listOf(first.toList(), second.toList()), FrameSplitter().frames(first + second))
  }

  @Test
  fun `a frame split inside its length comes out once it is whole`() {
    val splitter = FrameSplitter()
    assertEquals(emptyList(), splitter.frames(first.copyOfRange(0, 3)))
    assertEquals(listOf(first.toList()), splitter.frames(first.copyOfRange(3, first.size)))
  }

  @Test
  fun `a chunk ending mid-frame keeps the rest for the next push`() {
    val splitter = FrameSplitter()
    val bytes = first + second
    assertEquals(listOf(first.toList()), splitter.frames(bytes.copyOfRange(0, 12)))
    assertEquals(listOf(second.toList()), splitter.frames(bytes.copyOfRange(12, bytes.size)))
  }
}
