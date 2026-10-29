package to.holepunch.bare.rpc

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

private val peerScript = System.getProperty("bare-rpc.peer")

internal fun withPeer(
  onRequest: (suspend (IncomingRequest) -> Reply)? = null,
  onEvent: suspend (IncomingEvent) -> Unit = {},
  block: suspend (Rpc) -> Unit,
): Unit = runBlocking {
  val process =
    ProcessBuilder("node", peerScript).redirectError(ProcessBuilder.Redirect.INHERIT).start()
  val input = process.outputStream
  val closing = AtomicBoolean(false)
  val write: suspend (ByteArray) -> Unit = { bytes ->
    withContext(Dispatchers.IO) {
      input.write(bytes)
      input.flush()
    }
  }
  val data = flow {
    val buffer = ByteArray(64 * 1024)
    while (true) {
      val read = process.inputStream.read(buffer)
      if (read < 0) {
        check(closing.get()) { "peer exited with ${process.waitFor()}" }
        break
      }
      emit(buffer.copyOf(read))
    }
  }
    .flowOn(Dispatchers.IO)
  val rpc =
    if (onRequest == null) Rpc(data, write, this, onEvent)
    else Rpc(data, write, this, onEvent, onRequest)
  try {
    withTimeout(10.seconds) { block(rpc) }
  } finally {
    closing.set(true)
    input.close()
    if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly().waitFor()
  }
}
