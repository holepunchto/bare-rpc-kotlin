package to.holepunch.barerpc

import to.holepunch.compactencoding.decode
import to.holepunch.compactencoding.encode

/**
 * One end of a bare-rpc connection.
 *
 * @param send writes one encoded frame to the peer.
 * @param onEvent receives each event the peer sends.
 * @param maxFrameSize the longest frame [receive] accepts, length prefix included.
 */
public class BareRpc(
  private val send: suspend (ByteArray) -> Unit,
  private val onEvent: suspend (IncomingEvent) -> Unit = {},
  maxFrameSize: UInt = DEFAULT_MAX_FRAME_SIZE,
) {
  private val splitter = FrameSplitter(maxFrameSize)
  private var failure: RpcException? = null

  /** Sends [data] to the peer as an event for [command], which gets no reply. */
  public suspend fun event(command: ULong, data: ByteArray) {
    send(encode(frame, RequestMessage(0u, command, 0u, data)))
  }

  /**
   * Reads [bytes] received from the peer, in arrival order, and delivers each whole frame.
   *
   * @throws RpcException if a frame cannot be read. The connection is then closed, and every later
   *   call throws the same exception.
   */
  public suspend fun receive(bytes: ByteArray) {
    failure?.let { throw it }
    try {
      for (bytes in splitter.push(bytes)) dispatch(decode(frame, bytes))
    } catch (e: RpcException) {
      failure = e
      throw e
    }
  }

  private suspend fun dispatch(message: Message) {
    if (message is RequestMessage && message.id == 0uL) {
      onEvent(IncomingEvent(message.command, message.data ?: ByteArray(0)))
    }
  }
}

/**
 * An event received from the peer.
 *
 * @property command the command the event names.
 * @property data the event's payload.
 */
public class IncomingEvent
internal constructor(public val command: ULong, public val data: ByteArray)
