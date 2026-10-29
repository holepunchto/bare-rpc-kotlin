package to.holepunch.bare.rpc

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent

/** The peer of a [Rpc] under test, which runs the test until idle after each chunk it sends. */
@OptIn(ExperimentalCoroutinesApi::class)
internal class Remote(private val test: TestScope) {
  private val chunks = Channel<ByteArray>(Channel.UNLIMITED)

  val data: Flow<ByteArray> = chunks.consumeAsFlow()

  fun send(bytes: ByteArray) {
    chunks.trySend(bytes)
    test.runCurrent()
  }

  fun end() {
    chunks.close()
    test.runCurrent()
  }
}
