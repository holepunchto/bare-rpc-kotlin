package to.holepunch.barerpc

import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import to.holepunch.compactencoding.decode
import to.holepunch.compactencoding.encode

/**
 * One end of a bare-rpc connection.
 *
 * @param send writes one encoded frame to the peer, whole or not at all.
 * @param onEvent receives each event the peer sends.
 * @param onRequest answers each request the peer sends: its return value is the reply, and an
 *   exception it throws goes back as an error, a [RemoteException] as itself. By default every
 *   request is rejected as an unknown command.
 * @param maxFrameSize the longest frame [receive] accepts, length prefix included.
 */
public class BareRpc(
  private val send: suspend (ByteArray) -> Unit,
  private val onEvent: suspend (IncomingEvent) -> Unit = {},
  private val onRequest: suspend (IncomingRequest) -> ByteArray = {
    throw RemoteException("Unknown command ${it.command}", "ERR_UNKNOWN_COMMAND", 0)
  },
  maxFrameSize: UInt = DEFAULT_MAX_FRAME_SIZE,
) {
  private val splitter = FrameSplitter(maxFrameSize)
  private val sending = Mutex()
  private val state = Mutex()
  private var lastId = 0uL
  private val pending = mutableMapOf<ULong, CompletableDeferred<ByteArray>>()
  private var failure: Throwable? = null

  /**
   * Sends [data] to the peer as an event for [command], which gets no reply.
   *
   * @throws Throwable the failure that closed the connection, once [receive] has failed.
   */
  public suspend fun event(command: ULong, data: ByteArray) {
    sending.withLock {
      state.withLock { failure }?.let { throw it }
      send(encode(frame, RequestMessage(0u, command, 0u, data)))
    }
  }

  /**
   * Sends [data] to the peer as a request for [command] and returns the reply.
   *
   * @throws RemoteException if the peer replies with an error.
   * @throws Throwable the failure that closed the connection, if [receive] fails first, or the
   *   failure of [send], which fails only this request.
   */
  public suspend fun request(command: ULong, data: ByteArray): ByteArray {
    val reply = CompletableDeferred<ByteArray>()
    var id = 0uL
    try {
      sending.withLock {
        state.withLock {
          failure?.let { throw it }
          id = ++lastId
          pending[id] = reply
        }
        send(encode(frame, RequestMessage(id, command, 0u, data)))
      }
      return reply.await()
    } finally {
      withContext(NonCancellable) { state.withLock { pending.remove(id) } }
    }
  }

  /**
   * Reads [bytes] received from the peer, in arrival order, and delivers each whole frame. Calls
   * must not overlap, and must not come from [onEvent].
   *
   * @throws RpcException if a frame cannot be read. Any failure but cancellation, including one
   *   thrown by [onEvent], closes the connection: pending requests and every later call throw the
   *   same exception.
   */
  public suspend fun receive(bytes: ByteArray) {
    state.withLock { failure }?.let { throw it }
    try {
      for (frameBytes in splitter.push(bytes)) dispatch(decode(frame, frameBytes))
      splitter.push(ByteArray(0))
    } catch (e: CancellationException) {
      throw e
    } catch (e: Throwable) {
      val waiting = state.withLock {
        failure = e
        pending.values.toList().also { pending.clear() }
      }
      waiting.forEach { it.completeExceptionally(e) }
      throw e
    }
  }

  private suspend fun dispatch(message: Message) {
    when (message) {
      is RequestMessage -> {
        if (message.id != 0uL) return answer(message)
        if (message.stream != 0uL) throw MalformedFrameException("an event cannot open a stream")
        onEvent(IncomingEvent(message.command, message.data!!))
      }
      is ResponseMessage -> {
        val result = message.result
        if (result is ResponseResult.Success && message.stream != 0uL) TODO("response streams")
        val reply = state.withLock { pending.remove(message.id) } ?: return
        when (result) {
          is RemoteError ->
            reply.completeExceptionally(RemoteException(result.message, result.code, result.errno))
          is ResponseResult.Success -> reply.complete(result.data!!)
        }
      }
      is StreamMessage -> TODO("streams")
    }
  }

  private suspend fun answer(request: RequestMessage) {
    if (request.stream != 0uL) TODO("request streams")
    val result =
      try {
        ResponseResult.Success(onRequest(IncomingRequest(request.command, request.data!!)))
      } catch (e: CancellationException) {
        throw e
      } catch (e: RemoteException) {
        RemoteError(e.message.orEmpty(), e.code, e.errno)
      } catch (e: Throwable) {
        RemoteError(e.message.orEmpty(), "", 0)
      }
    sending.withLock { send(encode(frame, ResponseMessage(request.id, 0u, result))) }
  }
}

/**
 * A request received from the peer.
 *
 * @property command the command the request names.
 * @property data the request's payload.
 */
public class IncomingRequest
internal constructor(public val command: ULong, public val data: ByteArray)

/**
 * An event received from the peer.
 *
 * @property command the command the event names.
 * @property data the event's payload.
 */
public class IncomingEvent
internal constructor(public val command: ULong, public val data: ByteArray)
