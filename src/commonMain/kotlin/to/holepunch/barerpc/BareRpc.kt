package to.holepunch.barerpc

import kotlin.coroutines.cancellation.CancellationException
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
  private var failure: Throwable? = null

  /**
   * Sends [data] to the peer as an event for [command], which gets no reply.
   *
   * @throws Throwable the failure that closed the connection, once [receive] has failed.
   */
  public suspend fun event(command: ULong, data: ByteArray) {
    failure?.let { throw it }
    send(encode(frame, RequestMessage(0u, command, 0u, data)))
  }

  /**
   * Reads [bytes] received from the peer, in arrival order, and delivers each whole frame. Calls
   * must not overlap, and must not come from [onEvent].
   *
   * @throws RpcException if a frame cannot be read. Any failure but cancellation, including one
   *   thrown by [onEvent], closes the connection, and every later call throws the same exception.
   */
  public suspend fun receive(bytes: ByteArray) {
    failure?.let { throw it }
    try {
      for (frameBytes in splitter.push(bytes)) dispatch(decode(frame, frameBytes))
      splitter.push(ByteArray(0))
    } catch (e: CancellationException) {
      throw e
    } catch (e: Throwable) {
      failure = e
      throw e
    }
  }

  private suspend fun dispatch(message: Message) {
    if (message is RequestMessage && message.id == 0uL) {
      if (message.stream != 0uL) throw MalformedFrameException("an event cannot open a stream")
      onEvent(IncomingEvent(message.command, message.data!!))
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
