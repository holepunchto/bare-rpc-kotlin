package to.holepunch.barerpc

import to.holepunch.compactencoding.State
import to.holepunch.compactencoding.uint32

internal val DEFAULT_MAX_FRAME_SIZE: UInt = 16u * 1024u * 1024u

internal class FrameSplitter(private val maxFrameSize: UInt = DEFAULT_MAX_FRAME_SIZE) {
  private var buffered = ByteArray(0)

  fun push(bytes: ByteArray): List<ByteArray> {
    buffered += bytes
    val state = State(buffered)
    val frames = mutableListOf<ByteArray>()
    while (state.remaining >= 4) {
      val mark = state.start
      val length = uint32.decode(state)
      if (4uL + length > maxFrameSize) {
        if (frames.isEmpty()) {
          throw MalformedFrameException(
            "frame of ${4uL + length} bytes exceeds the cap of $maxFrameSize bytes"
          )
        }
        state.start = mark
        break
      }
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
