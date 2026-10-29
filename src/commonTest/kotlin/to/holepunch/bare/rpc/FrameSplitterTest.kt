package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

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
    assertEquals(listOf(first.toList()), splitter.frames(bytes.copyOfRange(0, 15)))
    assertEquals(listOf(second.toList()), splitter.frames(bytes.copyOfRange(15, bytes.size)))
  }

  @Test
  fun `a frame pushed in three parts comes out once it is whole`() {
    val splitter = FrameSplitter()
    assertEquals(emptyList(), splitter.frames(first.copyOfRange(0, 2)))
    assertEquals(emptyList(), splitter.frames(first.copyOfRange(2, 6)))
    assertEquals(listOf(first.toList()), splitter.frames(first.copyOfRange(6, first.size)))
  }

  @Test
  fun `a length past the default cap fails`() {
    assertFailsWith<MalformedFrameException> { FrameSplitter().push("ffffffff".hexToByteArray()) }
  }

  @Test
  fun `a frame longer than the cap fails as soon as its length arrives`() {
    assertFailsWith<MalformedFrameException> {
      FrameSplitter(maxFrameSize = 8u).push("05000000".hexToByteArray())
    }
  }

  @Test
  fun `a frame as long as the cap comes out`() {
    assertEquals(listOf(first.toList()), FrameSplitter(maxFrameSize = 9u).frames(first))
  }

  @Test
  fun `frames before a length past the cap come out before it fails`() {
    val splitter = FrameSplitter()
    assertEquals(listOf(first.toList()), splitter.frames(first + "ffffffff".hexToByteArray()))
    assertFailsWith<MalformedFrameException> { splitter.push(ByteArray(0)) }
  }
}
