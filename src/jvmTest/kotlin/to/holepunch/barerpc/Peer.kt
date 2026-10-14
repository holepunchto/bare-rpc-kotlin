package to.holepunch.barerpc

import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

private val peer = System.getProperty("bare-rpc.peer")

internal fun withPeer(block: suspend (BareRpc) -> Unit): Unit = runBlocking {
  val process = ProcessBuilder("node", peer).redirectError(ProcessBuilder.Redirect.INHERIT).start()
  val input = process.outputStream
  val rpc =
    BareRpc(
      send = { bytes ->
        withContext(Dispatchers.IO) {
          input.write(bytes)
          input.flush()
        }
      }
    )
  launch(Dispatchers.IO) {
    val buffer = ByteArray(64 * 1024)
    while (true) {
      val read = process.inputStream.read(buffer)
      if (read < 0) break
      rpc.receive(buffer.copyOf(read))
    }
  }
  try {
    withTimeout(10.seconds) { block(rpc) }
  } finally {
    input.close()
    if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly()
  }
}
