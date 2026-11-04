package to.holepunch.bare.rpc

import kotlinx.coroutines.flow.Flow

/**
 * A byte transport that an [Rpc] runs a session over. Create one from its parts with [Transport].
 */
public interface Transport {
  /** The bytes that arrive from the peer. The [Rpc] collects them once, and their end ends it. */
  public val data: Flow<ByteArray>

  /**
   * Writes one encoded frame to the peer, whole or not at all. It may suspend for backpressure, but
   * must not wait on [data] being collected. A failure fails the session.
   */
  public suspend fun write(data: ByteArray)

  /** Closes the transport. The [Rpc] calls it once, when the session ends. */
  public suspend fun close()
}

/**
 * Creates a [Transport] from its parts, such as `Transport(ipc.data, ipc::write, ipc::close)` for
 * the IPC of a worklet from bare-kit-kotlin.
 */
public fun Transport(
  data: Flow<ByteArray>,
  write: suspend (ByteArray) -> Unit,
  close: suspend () -> Unit,
): Transport = PartsTransport(data, write, close)

private class PartsTransport(
  override val data: Flow<ByteArray>,
  private val writer: suspend (ByteArray) -> Unit,
  private val closer: suspend () -> Unit,
) : Transport {
  override suspend fun write(data: ByteArray) = writer(data)

  override suspend fun close() = closer()
}
