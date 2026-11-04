package to.holepunch.bare.rpc

import to.holepunch.compactEncoding.State
import to.holepunch.compactEncoding.uint32

internal const val DEFAULT_MAX_FRAME_SIZE: Int = 16 * 1024 * 1024

internal class FrameSplitter(private val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE) {
  private var buffered = ByteArray(0)

  val hasPartialFrame: Boolean
    get() = buffered.isNotEmpty()

  fun push(bytes: ByteArray): List<ByteArray> {
    buffered += bytes
    val state = State(buffered)
    val frames = mutableListOf<ByteArray>()
    while (state.remaining >= 4) {
      val mark = state.start
      val length = uint32.decode(state)
      val size = 4L + length.toLong()
      if (size > maxFrameSize) {
        if (frames.isEmpty()) throw FrameTooLargeException(size, maxFrameSize)
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
