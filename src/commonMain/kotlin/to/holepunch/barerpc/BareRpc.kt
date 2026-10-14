package to.holepunch.barerpc

import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import to.holepunch.compactencoding.decode
import to.holepunch.compactencoding.encode

/**
 * One end of a bare-rpc connection.
 *
 * @param send writes one encoded frame to the peer, whole or not at all. A failure closes the
 *   connection.
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
    throw unknownCommand(it.command)
  },
  maxFrameSize: UInt = DEFAULT_MAX_FRAME_SIZE,
) {
  private val splitter = FrameSplitter(maxFrameSize)
  private val sending = Mutex()
  private val state = Mutex()
  private var lastId = 0uL
  private val pending = mutableMapOf<ULong, CompletableDeferred<ByteArray>>()
  private val outgoing = ArrayDeque<ByteArray>()
  private var failure: Throwable? = null

  /**
   * Sends [data] to the peer as an event for [command], which gets no reply.
   *
   * @throws Throwable the failure that closed the connection.
   */
  public suspend fun event(command: ULong, data: ByteArray) {
    state.withLock {
      failure?.let { throw it }
      outgoing.addLast(encode(frame, RequestMessage(0u, command, 0u, data)))
    }
    sending.withLock { drain() }
    flush()
  }

  /**
   * Sends [data] to the peer as a request for [command] and returns the reply.
   *
   * @throws RemoteException if the peer replies with an error.
   * @throws Throwable the failure that closed the connection.
   */
  public suspend fun request(command: ULong, data: ByteArray): ByteArray {
    val reply = CompletableDeferred<ByteArray>()
    var id = 0uL
    try {
      state.withLock {
        failure?.let { throw it }
        id = ++lastId
        pending[id] = reply
        outgoing.addLast(encode(frame, RequestMessage(id, command, 0u, data)))
      }
      sending.withLock { drain() }
      flush()
      return reply.await()
    } finally {
      withContext(NonCancellable) { state.withLock { pending.remove(id) } }
    }
  }

  /**
   * Reads [bytes] received from the peer, in arrival order, and delivers each whole frame. Calls
   * must not overlap, and must not come from [onEvent] or [onRequest]. Replies to requests never
   * wait for a send in progress.
   *
   * @throws RpcException if a frame cannot be read. Any failure closes the connection: one thrown
   *   by [onEvent], and one from [send]. Pending requests and every later call then throw the same
   *   exception. An exception from [onRequest] goes back to the peer instead, and cancelling the
   *   call leaves the connection open.
   */
  public suspend fun receive(bytes: ByteArray) {
    state.withLock { failure }?.let { throw it }
    try {
      for (frameBytes in splitter.push(bytes)) dispatch(decode(frame, frameBytes))
      splitter.push(ByteArray(0))
    } catch (e: Throwable) {
      if (e is CancellationException) currentCoroutineContext().ensureActive()
      close(e)
      throw e
    }
  }

  private suspend fun drain() {
    while (true) {
      val next =
        state.withLock {
          failure?.let { throw it }
          outgoing.removeFirstOrNull()
        } ?: return
      try {
        send(next)
      } catch (e: Throwable) {
        if (e is CancellationException) currentCoroutineContext().ensureActive()
        close(e)
        throw e
      }
    }
  }

  private suspend fun flush() {
    while (state.withLock { outgoing.isNotEmpty() } && sending.tryLock()) {
      try {
        drain()
      } finally {
        sending.unlock()
      }
    }
  }

  private suspend fun close(cause: Throwable) {
    val waiting = state.withLock {
      if (failure != null) return
      failure = cause
      outgoing.clear()
      pending.values.toList().also { pending.clear() }
    }
    waiting.forEach { it.completeExceptionally(cause) }
  }

  private suspend fun dispatch(message: Message) {
    when (message) {
      is RequestMessage ->
        if (message.id != 0uL) {
          answer(message)
        } else {
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
      } catch (e: RemoteException) {
        RemoteError(e.message.orEmpty(), e.code, e.errno)
      } catch (e: Throwable) {
        if (e is CancellationException) currentCoroutineContext().ensureActive()
        RemoteError(e.message.orEmpty(), "", 0)
      }
    state.withLock { outgoing.addLast(encode(frame, ResponseMessage(request.id, 0u, result))) }
    flush()
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
