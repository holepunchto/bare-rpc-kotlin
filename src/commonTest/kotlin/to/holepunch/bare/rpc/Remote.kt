package to.holepunch.bare.rpc

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.yield
import to.holepunch.compactEncoding.encode

/** The peer of a [Rpc] under test, which runs the test until idle after each chunk it sends. */
@OptIn(ExperimentalCoroutinesApi::class)
internal class Remote(private val test: TestScope) {
  private val chunks = Channel<ByteArray>(Channel.UNLIMITED)

  val data: Flow<ByteArray> = chunks.consumeAsFlow()

  fun send(bytes: ByteArray) {
    chunks.trySend(bytes)
    test.runCurrent()
  }

  fun send(message: Message) = send(encode(frame, message))

  fun end() {
    chunks.close()
    test.runCurrent()
  }
}

/** Throws the failure that closed the connection, if it has closed. */
internal suspend fun Rpc.assertOpen() = coroutineScope {
  val reply = async { request(0u, ByteArray(0)) }
  yield()
  if (reply.isCompleted) reply.await()
  reply.cancel()
}
