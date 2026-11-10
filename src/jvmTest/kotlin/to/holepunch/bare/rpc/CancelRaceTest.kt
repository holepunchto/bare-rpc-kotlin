package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import to.holepunch.compactEncoding.encode

class CancelRaceTest {
  private val gaveUp = RemoteError("Gave up", "GAVE_UP", 0)

  private fun Job.activeDescendants(): Int = children.sumOf {
    (if (it.isActive) 1 else 0) + it.activeDescendants()
  }

  @Test
  fun `a cancel racing a streamed reply leaves no sender behind`() = runBlocking {
    val parent = Job()
    val chunks = Channel<ByteArray>(Channel.UNLIMITED)
    val last = 5001uL
    val answered = CompletableDeferred<Unit>()
    Rpc(
      TestTransport(chunks.consumeAsFlow()) {
        if ((Messages.decode(it).message as? Message.Response)?.id == last) answered.complete(Unit)
      },
      scope = CoroutineScope(Dispatchers.Default + parent),
      onRequest = { if (it.command == 7uL) Reply.Stream(flow {}) else Reply.Data(ByteArray(0)) },
    )
    for (id in 1uL until last) {
      chunks.send(
        encode(frame, Message.Request(id, 7u, STREAM_OPEN, null)) +
          encode(frame, Message.Cancel(id, gaveUp))
      )
      if (id % 50uL == 0uL) delay(1)
    }
    chunks.send(encode(frame, Message.Request(last, 8u, 0u, ByteArray(0))))
    answered.await()
    // The connection's own job and its read are all that should be left.
    withTimeoutOrNull(2000) { while (parent.activeDescendants() > 2) delay(10) }
    assertEquals(2, parent.activeDescendants())
    parent.cancel()
  }

  @Test
  fun `a request read while the connection closes is not kept`() = runBlocking {
    val frames = (1..200).map { encode(frame, Message.Request(it.toULong(), 7u, 0u, ByteArray(0))) }
    val chunk = frames.fold(ByteArray(0), ByteArray::plus)
    repeat(500) {
      val parent = Job()
      val reading = CompletableDeferred<Unit>()
      val chunks = Channel<ByteArray>(Channel.UNLIMITED)
      val rpc =
        Rpc(
          TestTransport(chunks.consumeAsFlow()),
          scope = CoroutineScope(Dispatchers.Default + parent),
          onRequest = {
            reading.complete(Unit)
            awaitCancellation()
          },
        )
      chunks.send(chunk)
      // Closes while the rest of the chunk is read.
      reading.await()
      rpc.close()
      assertEquals(0, rpc.tracked())
      parent.cancel()
    }
  }
}
