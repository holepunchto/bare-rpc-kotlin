package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import to.holepunch.compactencoding.State
import to.holepunch.compactencoding.encode

class SequenceTest {
  @Test
  fun `every sequence decodes frame by frame and re-encodes to its bytes`() {
    for (family in sequenceFamilies) {
      val sequence = frameSequence(family)
      val state = State(sequence.bytes)
      val messages = buildList { while (state.remaining > 0) add(frame.decode(state)) }
      assertEquals(sequence.count, messages.size, family)
      assertContentEquals(
        sequence.bytes,
        messages.map { encode(frame, it) }.fold(ByteArray(0), ByteArray::plus),
        family,
      )
    }
  }
}
