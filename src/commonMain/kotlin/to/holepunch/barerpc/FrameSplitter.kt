package to.holepunch.barerpc

import to.holepunch.compactencoding.State
import to.holepunch.compactencoding.uint32

internal class FrameSplitter {
  private var buffered = ByteArray(0)

  fun push(bytes: ByteArray): List<ByteArray> {
    buffered += bytes
    val frames = mutableListOf<ByteArray>()
    var start = 0
    while (buffered.size - start >= 4) {
      val end = start + 4 + lengthAt(start)
      if (end > buffered.size) break
      frames += buffered.copyOfRange(start, end.toInt())
      start = end.toInt()
    }
    buffered = buffered.copyOfRange(start, buffered.size)
    return frames
  }

  private fun lengthAt(start: Int): Long =
    uint32.decode(State(buffered).also { it.start = start }).toLong()
}
