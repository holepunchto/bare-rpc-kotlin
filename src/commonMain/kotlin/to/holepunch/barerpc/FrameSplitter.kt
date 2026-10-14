package to.holepunch.barerpc

import to.holepunch.compactencoding.State
import to.holepunch.compactencoding.uint32

internal class FrameSplitter {
  private var buffered = ByteArray(0)

  fun push(bytes: ByteArray): List<ByteArray> {
    buffered += bytes
    val state = State(buffered)
    val frames = mutableListOf<ByteArray>()
    while (state.remaining >= 4) {
      val mark = state.start
      val length = uint32.decode(state)
      if (length > state.remaining.toUInt()) {
        state.start = mark
        break
      }
      state.start += length.toInt()
      frames += buffered.copyOfRange(mark, state.start)
    }
    buffered = buffered.copyOfRange(state.start, buffered.size)
    return frames
  }
}
