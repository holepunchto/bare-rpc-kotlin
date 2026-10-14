package to.holepunch.barerpc

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

private val peerScript = System.getProperty("bare-rpc.peer")

internal fun withPeer(
  onRequest: suspend (IncomingRequest) -> ByteArray = { throw unknownCommand(it.command) },
  onEvent: suspend (IncomingEvent) -> Unit = {},
  block: suspend (BareRpc) -> Unit,
): Unit = runBlocking {
  val process =
    ProcessBuilder("node", peerScript).redirectError(ProcessBuilder.Redirect.INHERIT).start()
  val input = process.outputStream
  val closing = AtomicBoolean(false)
  val rpc =
    BareRpc(
      send = { bytes ->
        withContext(Dispatchers.IO) {
          input.write(bytes)
          input.flush()
        }
      },
      onEvent = onEvent,
      onRequest = onRequest,
    )
  launch(Dispatchers.IO) {
    val buffer = ByteArray(64 * 1024)
    while (true) {
      val read = process.inputStream.read(buffer)
      if (read < 0) {
        check(closing.get()) { "peer exited with ${process.waitFor()}" }
        break
      }
      rpc.receive(buffer.copyOf(read))
    }
  }
  try {
    withTimeout(10.seconds) { block(rpc) }
  } finally {
    closing.set(true)
    input.close()
    if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly().waitFor()
  }
}
