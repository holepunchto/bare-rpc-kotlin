package to.holepunch.barerpc

import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
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
 *   connection. It is not cancelled once called, even when the caller that queued the frame is,
 *   since frames queued by other callers wait behind it.
 * @param onEvent receives each event the peer sends.
 * @param onRequest answers each request the peer sends: its return value is the reply, and an
 *   exception it throws goes back as an error, a [RemoteException] as itself. By default every
 *   request is rejected as an unknown command. A [Reply.Stream] streams from [scope].
 * @param maxFrameSize the longest frame [receive] accepts, length prefix included.
 * @param scope runs the handlers of requests that stream a body, and streams each [Reply.Stream],
 *   since both need later [receive] calls. Without a scope the peer gets an error instead, after
 *   the handler has run for a [Reply.Stream].
 */
public class BareRpc(
  private val send: suspend (ByteArray) -> Unit,
  private val onEvent: suspend (IncomingEvent) -> Unit = {},
  private val onRequest: suspend (IncomingRequest) -> Reply = { throw unknownCommand(it.command) },
  maxFrameSize: UInt = DEFAULT_MAX_FRAME_SIZE,
  private val scope: CoroutineScope? = null,
) {
  private val splitter = FrameSplitter(maxFrameSize)
  private val sending = Mutex()
  private val state = Mutex()
  private var lastId = 0uL
  private val pending = mutableMapOf<ULong, CompletableDeferred<ByteArray>>()
  private val requestAcks = mutableMapOf<ULong, CompletableDeferred<Boolean>>()
  private val requestBodies = mutableMapOf<ULong, Channel<ByteArray>>()
  private val replyBodies = mutableMapOf<ULong, Channel<ByteArray>>()
  private val replyAcks = mutableMapOf<ULong, CompletableDeferred<Unit>>()
  private val requestPauses = mutableMapOf<ULong, MutableStateFlow<Boolean>>()
  private val replyPauses = mutableMapOf<ULong, MutableStateFlow<Boolean>>()
  private val earlyReplyAcks = mutableSetOf<ULong>()
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
   * @throws UnexpectedStreamException if the peer replies with a stream.
   * @throws Throwable the failure that closed the connection, a [ConnectionClosedException] after
   *   [close].
   */
  public suspend fun request(command: ULong, data: ByteArray): ByteArray =
    sendRequest(command, data, null)

  /**
   * Sends a request for [command] whose payload is [body], streamed to the peer once it accepts the
   * stream, and returns the reply. Sending [body] stops once the reply comes, which skips it if the
   * peer has not accepted the stream yet.
   *
   * @throws RemoteException if the peer replies with an error.
   * @throws UnexpectedStreamException if the peer replies with a stream.
   * @throws Throwable the failure that closed the connection, a [ConnectionClosedException] after
   *   [close].
   */
  public suspend fun request(command: ULong, body: Flow<ByteArray>): ByteArray =
    sendRequest(command, null, body)

  /**
   * Sends [data] to the peer as a request for [command], and emits the reply: each chunk of a
   * streamed reply, or a single reply as one chunk. Each collection sends a new request.
   *
   * @throws RemoteException if the peer replies or closes the stream with an error.
   * @throws Throwable the failure that closed the connection, a [ConnectionClosedException] after
   *   [close].
   */
  public fun requestFlow(command: ULong, data: ByteArray): Flow<ByteArray> =
    replyFlow(command, data, null)

  /**
   * Sends a request for [command] whose payload is [body], streamed to the peer once it accepts the
   * stream, and emits the reply while [body] is sent: each chunk of a streamed reply, or a single
   * reply as one chunk. Sending [body] stops once a single reply comes or a streamed reply ends.
   * Each collection sends a new request.
   *
   * @throws RemoteException if the peer replies or closes the stream with an error.
   * @throws Throwable the failure that closed the connection, a [ConnectionClosedException] after
   *   [close].
   */
  public fun requestFlow(command: ULong, body: Flow<ByteArray>): Flow<ByteArray> =
    replyFlow(command, null, body)

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
    val paused = MutableStateFlow(false)
    var id = 0uL
    try {
      sendMessage {
        id = ++lastId
        pending[id] = reply
        if (body != null) {
          requestAcks[id] = accepted
          requestPauses[id] = paused
        }
        RequestMessage(id, command, if (body != null) STREAM_OPEN else 0u, data)
      }
      return coroutineScope {
        val sender = body?.let { launch { if (accepted.await()) sendRequestBody(id, it, paused) } }
        reply.await().also { sender?.cancel() }
      }
    } finally {
      withContext(NonCancellable) {
        state.withLock {
          pending.remove(id)
          requestAcks.remove(id)
          requestPauses.remove(id)
        }
      }
    }
  }

  private fun replyFlow(
    command: ULong,
    data: ByteArray?,
    body: Flow<ByteArray>?,
  ): Flow<ByteArray> = flow {
    val chunks = Channel<ByteArray>(Channel.UNLIMITED)
    val accepted = CompletableDeferred<Boolean>()
    val paused = MutableStateFlow(false)
    var id = 0uL
    try {
      sendMessage {
        id = ++lastId
        replyBodies[id] = chunks
        if (body != null) {
          requestAcks[id] = accepted
          requestPauses[id] = paused
        }
        RequestMessage(id, command, if (body != null) STREAM_OPEN else 0u, data)
      }
      sendMessage { StreamMessage(id, STREAM_RESPONSE or STREAM_OPEN, null, null) }
      coroutineScope {
        val sender = body?.let { launch { if (accepted.await()) sendRequestBody(id, it, paused) } }
        for (chunk in chunks) emit(chunk)
        sender?.cancel()
      }
    } finally {
      withContext(NonCancellable) {
        state.withLock {
          replyBodies.remove(id)
          requestAcks.remove(id)
          requestPauses.remove(id)
        }
      }
    }
  }

  private suspend fun sendRequestBody(
    id: ULong,
    body: Flow<ByteArray>,
    paused: MutableStateFlow<Boolean>,
  ) {
    body.collect {
      paused.awaitResume()
      sendMessage { StreamMessage(id, STREAM_REQUEST or STREAM_DATA, it, null) }
    }
    paused.awaitResume()
    sendMessage { StreamMessage(id, STREAM_REQUEST or STREAM_END, null, null) }
  }

  private suspend fun MutableStateFlow<Boolean>.awaitResume() {
    first { !it }
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
    withContext(NonCancellable) { sendQueued() }?.let { throw it }
  }

  private suspend fun sendQueued(): Throwable? {
    while (true) {
      val next =
        state.withLock {
          failure?.let {
            return it
          }
          outgoing.removeFirstOrNull()
        } ?: return null
      try {
        send(next)
      } catch (e: Throwable) {
        fail(e)
        return e
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
    val pauses: List<MutableStateFlow<Boolean>>
    state.withLock {
      if (failure != null) return
      failure = cause
      outgoing.clear()
      waiting = pending.values + requestAcks.values + replyAcks.values
      bodies = requestBodies.values + replyBodies.values
      pending.clear()
      requestAcks.clear()
      requestBodies.clear()
      replyBodies.clear()
      replyAcks.clear()
      pauses = requestPauses.values + replyPauses.values
      requestPauses.clear()
      replyPauses.clear()
      earlyReplyAcks.clear()
    }
    waiting.forEach { it.completeExceptionally(cause) }
    pauses.forEach { it.value = false }
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
        if (result is ResponseResult.Success && message.stream != 0uL) {
          val (reply, accepted) =
            state.withLock {
              if (message.id in replyBodies) return
              pending.remove(message.id) to requestAcks.remove(message.id)
            }
          accepted?.complete(false)
          reply?.completeExceptionally(UnexpectedStreamException())
          return
        }
        val reply: CompletableDeferred<ByteArray>?
        val accepted: CompletableDeferred<Boolean>?
        val chunks: Channel<ByteArray>?
        state.withLock {
          reply = pending.remove(message.id)
          accepted = requestAcks.remove(message.id)
          chunks = replyBodies.remove(message.id)
        }
        accepted?.complete(false)
        when (result) {
          is RemoteError -> {
            val error = remoteException(result)
            reply?.completeExceptionally(error)
            chunks?.close(error)
          }
          is ResponseResult.Success -> {
            reply?.complete(result.data!!)
            chunks?.trySend(result.data!!)
            chunks?.close()
          }
        }
      }
      is StreamMessage -> {
        if (message.id == 0uL) return
        val event = streamEvents.firstOrNull { message.flags and it != 0uL } ?: return
        when {
          message.flags and STREAM_REQUEST != 0uL -> onRequestStream(message, event)
          message.flags and STREAM_RESPONSE != 0uL -> onReplyStream(message, event)
          else -> TODO("streams")
        }
      }
    }
  }

  private suspend fun onRequestStream(message: StreamMessage, event: ULong) {
    when {
      event == STREAM_OPEN -> state.withLock { requestAcks.remove(message.id) }?.complete(true)
      event == STREAM_PAUSE || event == STREAM_RESUME ->
        state.withLock { requestPauses[message.id] }?.value = event == STREAM_PAUSE
      event == STREAM_DATA -> state.withLock { requestBodies[message.id] }?.trySend(message.data!!)
      event == STREAM_END || event == STREAM_CLOSE && message.error == null ->
        state.withLock { requestBodies[message.id] }?.close()
      event == STREAM_DESTROY && message.error == null -> Unit
      else -> TODO("streams")
    }
  }

  private suspend fun onReplyStream(message: StreamMessage, event: ULong) {
    when {
      event == STREAM_OPEN -> acceptReply(message.id)
      event == STREAM_PAUSE || event == STREAM_RESUME ->
        state.withLock { replyPauses[message.id] }?.value = event == STREAM_PAUSE
      event == STREAM_DATA -> state.withLock { replyBodies[message.id] }?.trySend(message.data!!)
      event == STREAM_END || event == STREAM_CLOSE -> {
        val (chunks, accepted) =
          state.withLock { replyBodies.remove(message.id) to requestAcks.remove(message.id) }
        accepted?.complete(false)
        chunks?.close(message.error?.let(::remoteException))
      }
      event == STREAM_DESTROY && message.error == null -> Unit
      else -> TODO("streams")
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
    launchStream(scope, { requestBodies.remove(request.id) }) {
      reply(request.id) { onRequest(IncomingRequest(request.command, ByteArray(0), body)) }
    }
  }

  private suspend fun reply(id: ULong, handler: suspend () -> Reply) {
    val reply =
      try {
        handler()
      } catch (e: Throwable) {
        if (e is CancellationException) currentCoroutineContext().ensureActive()
        return respond(id, 0u, remoteError(e))
      }
    when (reply) {
      is Reply.Data -> respond(id, 0u, ResponseResult.Success(reply.data))
      is Reply.Stream -> streamReply(id, reply.body)
    }
  }

  private suspend fun streamReply(id: ULong, body: Flow<ByteArray>) {
    val scope = scope ?: return reply(id) { throw noScope() }
    val accepted = CompletableDeferred<Unit>()
    val paused = MutableStateFlow(false)
    state.withLock {
      failure?.let { throw it }
      if (earlyReplyAcks.remove(id)) accepted.complete(Unit) else replyAcks[id] = accepted
      replyPauses[id] = paused
    }
    respond(id, STREAM_OPEN, ResponseResult.Success(null))
    launchStream(
      scope,
      {
        replyAcks.remove(id)
        replyPauses.remove(id)
      },
    ) {
      accepted.await()
      var error: Throwable? = null
      body
        .catch { error = it }
        .collect {
          paused.awaitResume()
          sendMessage { StreamMessage(id, STREAM_RESPONSE or STREAM_DATA, it, null) }
        }
      val failed = error
      paused.awaitResume()
      sendMessage {
        if (failed == null) StreamMessage(id, STREAM_RESPONSE or STREAM_END, null, null)
        else
          StreamMessage(
            id,
            STREAM_RESPONSE or STREAM_CLOSE or STREAM_ERROR,
            null,
            remoteError(failed),
          )
      }
    }
  }

  private fun launchStream(scope: CoroutineScope, cleanup: () -> Unit, block: suspend () -> Unit) =
    scope.launch {
      try {
        block()
      } catch (e: Throwable) {
        // Every error here is the connection's failure, which receive and request already throw.
        if (e is CancellationException) currentCoroutineContext().ensureActive()
      } finally {
        withContext(NonCancellable) { state.withLock { cleanup() } }
      }
    }

  private suspend fun acceptReply(id: ULong) {
    val accepted = state.withLock {
      val accepted = replyAcks.remove(id)
      if (accepted == null) earlyReplyAcks += id
      accepted
    }
    accepted?.complete(Unit)
  }

  private suspend fun respond(id: ULong, stream: ULong, result: ResponseResult) {
    state.withLock {
      if (stream == 0uL) earlyReplyAcks.remove(id)
      outgoing.addLast(encode(frame, ResponseMessage(id, stream, result)))
    }
    flush()
  }

  private fun remoteError(e: Throwable) =
    if (e is RemoteException) RemoteError(e.message.orEmpty(), e.code, e.errno)
    else RemoteError(e.message.orEmpty(), "", 0)
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
