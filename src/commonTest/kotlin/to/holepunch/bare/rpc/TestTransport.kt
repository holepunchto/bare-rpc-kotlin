package to.holepunch.bare.rpc

import kotlinx.coroutines.flow.Flow

internal class TestTransport(
  override val data: Flow<ByteArray>,
  private val onWrite: suspend (ByteArray) -> Unit = {},
) : Transport {
  var closes = 0

  override suspend fun write(data: ByteArray) = onWrite(data)

  override suspend fun close() {
    closes++
  }
}
