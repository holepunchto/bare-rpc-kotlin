package to.holepunch.barerpc

import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.launch
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
 * @param scope runs the handlers of requests that stream a body, which need later [receive] calls
 *   to read it. Without a scope such requests are rejected.
 */
public class BareRpc(
  private val send: suspend (ByteArray) -> Unit,
  private val onEvent: suspend (IncomingEvent) -> Unit = {},
  private val onRequest: suspend (IncomingRequest) -> ByteArray = {
    throw unknownCommand(it.command)
  },
  maxFrameSize: UInt = DEFAULT_MAX_FRAME_SIZE,
  private val scope: CoroutineScope? = null,
) {
  private val splitter = FrameSplitter(maxFrameSize)
  private val sending = Mutex()
  private val state = Mutex()
  private var lastId = 0uL
  private val pending = mutableMapOf<ULong, CompletableDeferred<ByteArray>>()
  private val acks = mutableMapOf<ULong, CompletableDeferred<Boolean>>()
  private val requestBodies = mutableMapOf<ULong, Channel<ByteArray>>()
  private val outgoing = ArrayDeque<ByteArray>()
  private var failure: Throwable? = null

  /**
   * Sends [data] to the peer as an event for [command], which gets no reply.
   *
   * @throws Throwable the failure that closed the connection, a [ConnectionClosedException] after
   *   [close].
   */
  public suspend fun event(command: ULong, data: ByteArray) {
    sendMessage { RequestMessage(0u, command, 0u, data) }
  }

  /**
   * Sends [data] to the peer as a request for [command] and returns the reply.
   *
   * @throws RemoteException if the peer replies with an error.
   * @throws Throwable the failure that closed the connection, a [ConnectionClosedException] after
   *   [close].
   */
  public suspend fun request(command: ULong, data: ByteArray): ByteArray =
    sendRequest(command, data, null)

  /**
   * Sends a request for [command] whose payload is [body], streamed to the peer once it accepts the
   * stream, and returns the reply. A reply that comes before the peer accepts the stream skips
   * [body].
   *
   * @throws RemoteException if the peer replies with an error.
   * @throws Throwable the failure that closed the connection, a [ConnectionClosedException] after
   *   [close].
   */
  public suspend fun request(command: ULong, body: Flow<ByteArray>): ByteArray =
    sendRequest(command, null, body)

  /**
   * Reads [bytes] received from the peer, in arrival order, and delivers each whole frame. Calls
   * must not overlap, and must not come from [onEvent] or [onRequest]. Replies to requests never
   * wait for a send in progress.
   *
   * @throws RpcException if a frame cannot be read. Any failure closes the connection: one thrown
   *   by [onEvent], and one from [send]. Pending requests and every later call then throw the same
   *   exception. An exception from [onRequest] goes back to the peer instead, and cancelling the
   *   call leaves the connection open. After [close], including one from a handler, the rest of the
   *   bytes go undelivered and this throws a [ConnectionClosedException].
   */
  public suspend fun receive(bytes: ByteArray) {
    state.withLock { failure }?.let { throw it }
    try {
      for (frameBytes in splitter.push(bytes)) {
        state.withLock { failure }?.let { throw it }
        dispatch(decode(frame, frameBytes))
      }
      splitter.push(ByteArray(0))
    } catch (e: Throwable) {
      if (e is CancellationException) currentCoroutineContext().ensureActive()
      fail(e)
      throw e
    }
  }

  /**
   * Closes the connection: pending requests and every later call throw a
   * [ConnectionClosedException]. Closing after a failure keeps that failure. The transport stays
   * open; the caller closes it.
   */
  public suspend fun close() {
    fail(ConnectionClosedException())
  }

  private suspend fun sendRequest(
    command: ULong,
    data: ByteArray?,
    body: Flow<ByteArray>?,
  ): ByteArray {
    val reply = CompletableDeferred<ByteArray>()
    val accepted = CompletableDeferred<Boolean>()
    var id = 0uL
    try {
      sendMessage {
        id = ++lastId
        pending[id] = reply
        if (body != null) acks[id] = accepted
        RequestMessage(id, command, if (body != null) STREAM_OPEN else 0u, data)
      }
      if (body != null && accepted.await()) {
        body.collect { sendMessage { StreamMessage(id, STREAM_REQUEST or STREAM_DATA, it, null) } }
        sendMessage { StreamMessage(id, STREAM_REQUEST or STREAM_END, null, null) }
      }
      return reply.await()
    } finally {
      withContext(NonCancellable) {
        state.withLock {
          pending.remove(id)
          acks.remove(id)
        }
      }
    }
  }

  private suspend fun sendMessage(message: () -> Message) {
    state.withLock {
      failure?.let { throw it }
      outgoing.addLast(encode(frame, message()))
    }
    sending.withLock { drain() }
    flush()
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
        fail(e)
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

  private suspend fun fail(cause: Throwable) {
    val waiting: List<CompletableDeferred<*>>
    val bodies: List<Channel<ByteArray>>
    state.withLock {
      if (failure != null) return
      failure = cause
      outgoing.clear()
      waiting = pending.values + acks.values
      bodies = requestBodies.values.toList()
      pending.clear()
      acks.clear()
      requestBodies.clear()
    }
    waiting.forEach { it.completeExceptionally(cause) }
    bodies.forEach { it.close(cause) }
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
        val (reply, accepted) =
          state.withLock { pending.remove(message.id) to acks.remove(message.id) }
        accepted?.complete(false)
        if (reply == null) return
        when (result) {
          is RemoteError ->
            reply.completeExceptionally(RemoteException(result.message, result.code, result.errno))
          is ResponseResult.Success -> reply.complete(result.data!!)
        }
      }
      is StreamMessage -> {
        if (message.id == 0uL) return
        val event = streamEvents.firstOrNull { message.flags and it != 0uL } ?: return
        when {
          message.flags and STREAM_REQUEST == 0uL -> TODO("streams")
          event == STREAM_OPEN -> state.withLock { acks.remove(message.id) }?.complete(true)
          event == STREAM_PAUSE || event == STREAM_RESUME -> Unit
          event == STREAM_DATA ->
            state.withLock { requestBodies[message.id] }?.trySend(message.data!!)
          event == STREAM_END || event == STREAM_CLOSE && message.error == null ->
            state.withLock { requestBodies[message.id] }?.close()
          event == STREAM_DESTROY && message.error == null -> Unit
          else -> TODO("streams")
        }
      }
    }
  }

  private suspend fun answer(request: RequestMessage) {
    if (request.stream == 0uL) {
      return reply(request.id) { onRequest(IncomingRequest(request.command, request.data!!, null)) }
    }
    val scope = scope ?: return reply(request.id) { throw noScope() }
    val chunks = Channel<ByteArray>(Channel.UNLIMITED)
    state.withLock {
      failure?.let { throw it }
      requestBodies[request.id] = chunks
    }
    val body =
      chunks.consumeAsFlow().onStart {
        sendMessage { StreamMessage(request.id, STREAM_REQUEST or STREAM_OPEN, null, null) }
      }
    scope.launch {
      try {
        reply(request.id) { onRequest(IncomingRequest(request.command, ByteArray(0), body)) }
      } catch (e: Throwable) {
        // Every error here is the connection's failure, which receive and request already throw.
        if (e is CancellationException) currentCoroutineContext().ensureActive()
      } finally {
        withContext(NonCancellable) { state.withLock { requestBodies.remove(request.id) } }
      }
    }
  }

  private suspend fun reply(id: ULong, handler: suspend () -> ByteArray) {
    val result =
      try {
        ResponseResult.Success(handler())
      } catch (e: RemoteException) {
        RemoteError(e.message.orEmpty(), e.code, e.errno)
      } catch (e: Throwable) {
        if (e is CancellationException) currentCoroutineContext().ensureActive()
        RemoteError(e.message.orEmpty(), "", 0)
      }
    state.withLock { outgoing.addLast(encode(frame, ResponseMessage(id, 0u, result))) }
    flush()
  }
}

/**
 * A request received from the peer.
 *
 * @property command the command the request names.
 * @property data the request's payload, empty when it streams a [body].
 * @property body the request's streamed payload, or null. Collecting it accepts the stream, and it
 *   can be collected once.
 */
public class IncomingRequest
internal constructor(
  public val command: ULong,
  public val data: ByteArray,
  public val body: Flow<ByteArray>?,
)

/**
 * An event received from the peer.
 *
 * @property command the command the event names.
 * @property data the event's payload.
 */
public class IncomingEvent
internal constructor(public val command: ULong, public val data: ByteArray)
