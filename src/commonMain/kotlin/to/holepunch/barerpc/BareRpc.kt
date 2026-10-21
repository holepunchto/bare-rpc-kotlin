package to.holepunch.barerpc

import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Job
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
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.job
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
 * @param maxBufferedBytes the most of one stream's data held for its collector. A peer that sends
 *   past it fails the connection with a [StreamOverflowException]. Below the 16 KiB pause mark, the
 *   peer is never paused first.
 * @param scope runs the handlers of requests that stream a body, and streams each [Reply.Stream],
 *   since both need later [receive] calls. Without a scope the peer gets an error instead, after
 *   the handler has run for a [Reply.Stream].
 */
public class BareRpc(
  private val send: suspend (ByteArray) -> Unit,
  private val onEvent: suspend (IncomingEvent) -> Unit = {},
  private val onRequest: suspend (IncomingRequest) -> Reply = { throw unknownCommand(it.command) },
  maxFrameSize: UInt = DEFAULT_MAX_FRAME_SIZE,
  private val maxBufferedBytes: UInt = DEFAULT_MAX_BUFFERED_BYTES,
  private val scope: CoroutineScope? = null,
) {
  private val splitter = FrameSplitter(maxFrameSize)
  private val sending = Mutex()
  private val state = Mutex()
  private var lastId = 0uL
  private val pending = mutableMapOf<ULong, CompletableDeferred<ByteArray>>()
  private val requestAcks = mutableMapOf<ULong, CompletableDeferred<Boolean>>()
  private val requestBodies = mutableMapOf<ULong, Inbox>()
  private val replyBodies = mutableMapOf<ULong, Inbox>()
  private val replyAcks = mutableMapOf<ULong, CompletableDeferred<Unit>>()
  private val requestPauses = mutableMapOf<ULong, MutableStateFlow<Boolean>>()
  private val replyPauses = mutableMapOf<ULong, MutableStateFlow<Boolean>>()
  private val requestSenders = mutableMapOf<ULong, Job>()
  private val replySenders = mutableMapOf<ULong, Job>()
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
    sendMessage { Message.Request(0u, command, 0u, data) }
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
   * stream, and returns the reply. Sending [body] stops once the reply comes or the peer stops
   * reading it, and a reply before the peer accepts the stream skips [body]. If [body] throws, the
   * stream is closed with the error and this call throws it.
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
   * streamed reply, or a single reply as one chunk. Each collection sends a new request. Stopping a
   * collection before a streamed reply ends tells a peer that has started streaming to stop.
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
   * reply as one chunk. Sending [body] stops once a single reply comes, a streamed reply ends, or
   * the peer stops reading it. If [body] throws, the stream is closed with the error and the flow
   * throws it. Each collection sends a new request. Stopping a collection before a streamed reply
   * ends tells a peer that has started streaming to stop.
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
        Message.Request(id, command, if (body != null) STREAM_OPEN else 0u, data)
      }
      return coroutineScope {
        val sender = body?.let { launch { sendRequestBody(id, it, accepted, paused) } }
        reply.await().also { sender?.cancel() }
      }
    } finally {
      withContext(NonCancellable) {
        state.withLock {
          pending.remove(id)
          requestAcks.remove(id)
          requestPauses.remove(id)
          requestSenders.remove(id)
        }
      }
    }
  }

  private fun replyFlow(
    command: ULong,
    data: ByteArray?,
    body: Flow<ByteArray>?,
  ): Flow<ByteArray> = flow {
    val inbox = Inbox(STREAM_RESPONSE, maxBufferedBytes)
    val accepted = CompletableDeferred<Boolean>()
    val paused = MutableStateFlow(false)
    var id = 0uL
    try {
      sendMessage {
        id = ++lastId
        replyBodies[id] = inbox
        if (body != null) {
          requestAcks[id] = accepted
          requestPauses[id] = paused
        }
        Message.Request(id, command, if (body != null) STREAM_OPEN else 0u, data)
      }
      sendMessage {
        inbox.opened = true
        Message.Stream(id, STREAM_RESPONSE or STREAM_OPEN, null, null)
      }
      coroutineScope {
        val sender = body?.let { launch { sendRequestBody(id, it, accepted, paused) } }
        for (chunk in inbox.chunks) {
          take(id, inbox, chunk)
          emit(chunk)
        }
        sender?.cancel()
      }
    } finally {
      stopReading(id, replyBodies)
      withContext(NonCancellable) {
        state.withLock {
          requestAcks.remove(id)
          requestPauses.remove(id)
          requestSenders.remove(id)
        }
      }
    }
  }

  private suspend fun sendRequestBody(
    id: ULong,
    body: Flow<ByteArray>,
    accepted: CompletableDeferred<Boolean>,
    paused: MutableStateFlow<Boolean>,
  ) {
    state.withLock { requestSenders[id] = currentCoroutineContext().job }
    if (!accepted.await()) return
    sendBody(id, STREAM_REQUEST, body, paused)?.let { throw it }
  }

  private suspend fun sendBody(
    id: ULong,
    mask: ULong,
    body: Flow<ByteArray>,
    paused: MutableStateFlow<Boolean>,
  ): Throwable? {
    var error: Throwable? = null
    body
      .catch { error = it }
      .collect {
        paused.awaitResume()
        sendMessage { Message.Stream(id, mask or STREAM_DATA, it, null) }
      }
    val failed = error
    paused.awaitResume()
    sendMessage {
      if (failed == null) Message.Stream(id, mask or STREAM_END, null, null)
      else Message.Stream(id, mask or STREAM_CLOSE or STREAM_ERROR, null, remoteError(failed))
    }
    return failed
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
      bodies = (requestBodies.values + replyBodies.values).map { it.chunks }
      pending.clear()
      requestAcks.clear()
      requestBodies.clear()
      replyBodies.clear()
      replyAcks.clear()
      pauses = requestPauses.values + replyPauses.values
      requestPauses.clear()
      replyPauses.clear()
      requestSenders.clear()
      replySenders.clear()
      earlyReplyAcks.clear()
    }
    waiting.forEach { it.completeExceptionally(cause) }
    pauses.forEach { it.value = false }
    bodies.forEach { it.close(cause) }
  }

  private suspend fun dispatch(message: Message) {
    when (message) {
      is Message.Request ->
        if (message.id != 0uL) {
          answer(message)
        } else {
          if (message.stream != 0uL) throw MalformedFrameException("an event cannot open a stream")
          onEvent(IncomingEvent(message.command, message.data!!))
        }
      is Message.Response -> {
        val error = message.error
        if (error == null && message.stream != 0uL) {
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
          chunks = replyBodies.remove(message.id)?.chunks
        }
        accepted?.complete(false)
        if (error != null) {
          val exception = remoteException(error)
          reply?.completeExceptionally(exception)
          chunks?.close(exception)
        } else {
          reply?.complete(message.data!!)
          chunks?.trySend(message.data!!)
          chunks?.close()
        }
      }
      is Message.Stream -> {
        if (message.id == 0uL) return
        val flag = streamEvents.firstOrNull { message.flags and it != 0uL } ?: return
        val event = if (flag == STREAM_DATA && message.data == null) STREAM_END else flag
        when {
          message.flags and STREAM_REQUEST != 0uL -> onRequestStream(message, event)
          message.flags and STREAM_RESPONSE != 0uL -> onReplyStream(message, event)
          else -> Unit
        }
      }
    }
  }

  private suspend fun onRequestStream(message: Message.Stream, event: ULong) {
    when {
      event == STREAM_OPEN -> state.withLock { requestAcks.remove(message.id) }?.complete(true)
      event == STREAM_PAUSE || event == STREAM_RESUME ->
        state.withLock { requestPauses[message.id] }?.value = event == STREAM_PAUSE
      event == STREAM_DATA ->
        state
          .withLock { requestBodies[message.id] }
          ?.let { deliver(message.id, it, message.data!!) }
      event == STREAM_END -> state.withLock { requestBodies.remove(message.id) }?.chunks?.close()
      event == STREAM_CLOSE ->
        state
          .withLock { requestBodies.remove(message.id) }
          ?.chunks
          ?.close(message.error?.let(::remoteException))
      event == STREAM_DESTROY -> state.withLock { requestSenders.remove(message.id) }?.cancel()
    }
  }

  private suspend fun onReplyStream(message: Message.Stream, event: ULong) {
    when {
      event == STREAM_OPEN -> acceptReply(message.id)
      event == STREAM_PAUSE || event == STREAM_RESUME ->
        state.withLock { replyPauses[message.id] }?.value = event == STREAM_PAUSE
      event == STREAM_DATA ->
        state.withLock { replyBodies[message.id] }?.let { deliver(message.id, it, message.data!!) }
      event == STREAM_END || event == STREAM_CLOSE -> {
        val (chunks, accepted) =
          state.withLock {
            replyBodies.remove(message.id)?.chunks to requestAcks.remove(message.id)
          }
        accepted?.complete(false)
        chunks?.close(if (event == STREAM_CLOSE) message.error?.let(::remoteException) else null)
      }
      event == STREAM_DESTROY -> state.withLock { replySenders.remove(message.id) }?.cancel()
    }
  }

  private suspend fun answer(request: Message.Request) {
    if (request.stream == 0uL) {
      reply(request.id) { onRequest(IncomingRequest(request.command, request.data!!, null)) }
      return
    }
    val scope = scope
    if (scope == null) {
      reply(request.id) { throw noScope() }
      return
    }
    val inbox = Inbox(STREAM_REQUEST, maxBufferedBytes)
    state.withLock {
      failure?.let { throw it }
      requestBodies[request.id] = inbox
    }
    val body =
      inbox.chunks
        .consumeAsFlow()
        .onStart {
          queueSignals(request.id) {
            if (requestBodies[request.id] !== inbox) return@queueSignals emptyList()
            inbox.opened = true
            listOf(STREAM_REQUEST or STREAM_OPEN)
          }
        }
        .onEach { take(request.id, inbox, it) }
        .onCompletion { stopReading(request.id, requestBodies) }
    launchStream(scope, { stopReading(request.id, requestBodies) }) {
      reply(request.id) { onRequest(IncomingRequest(request.command, ByteArray(0), body)) }?.join()
    }
  }

  private suspend fun reply(id: ULong, handler: suspend () -> Reply): Job? {
    val reply =
      try {
        handler()
      } catch (e: Throwable) {
        if (e is CancellationException) currentCoroutineContext().ensureActive()
        respond(id, 0u, error = remoteError(e))
        return null
      }
    return when (reply) {
      is Reply.Data -> {
        respond(id, 0u, data = reply.data)
        null
      }
      is Reply.Stream -> streamReply(id, reply.body)
    }
  }

  private suspend fun streamReply(id: ULong, body: Flow<ByteArray>): Job? {
    val scope = scope ?: return reply(id) { throw noScope() }
    val accepted = CompletableDeferred<Unit>()
    val paused = MutableStateFlow(false)
    state.withLock {
      failure?.let { throw it }
      if (earlyReplyAcks.remove(id)) accepted.complete(Unit) else replyAcks[id] = accepted
      replyPauses[id] = paused
    }
    respond(id, STREAM_OPEN)
    return launchStream(
      scope,
      {
        state.withLock {
          replyAcks.remove(id)
          replyPauses.remove(id)
          replySenders.remove(id)
        }
      },
    ) {
      state.withLock { replySenders[id] = currentCoroutineContext().job }
      accepted.await()
      sendBody(id, STREAM_RESPONSE, body, paused)
    }
  }

  @OptIn(DelicateCoroutinesApi::class)
  private fun launchStream(
    scope: CoroutineScope,
    cleanup: suspend () -> Unit,
    block: suspend () -> Unit,
  ) =
    // Starts even on a cancelled scope, so cleanup always runs.
    scope.launch(start = CoroutineStart.ATOMIC) {
      try {
        currentCoroutineContext().ensureActive()
        block()
      } catch (e: Throwable) {
        // Every error here is the connection's failure, which receive and request already throw.
        if (e is CancellationException) currentCoroutineContext().ensureActive()
      } finally {
        withContext(NonCancellable) { cleanup() }
      }
    }

  private suspend fun deliver(id: ULong, inbox: Inbox, data: ByteArray) {
    queueSignals(id) { listOfNotNull((inbox.mask or STREAM_PAUSE).takeIf { inbox.add(data.size) }) }
    inbox.chunks.trySend(data)
  }

  private suspend fun take(id: ULong, inbox: Inbox, data: ByteArray) {
    queueSignals(id) {
      listOfNotNull((inbox.mask or STREAM_RESUME).takeIf { inbox.remove(data.size) })
    }
  }

  private suspend fun stopReading(id: ULong, inboxes: MutableMap<ULong, Inbox>) =
    withContext(NonCancellable) {
      queueSignals(id) {
        val inbox = inboxes.remove(id) ?: return@queueSignals emptyList()
        inbox.chunks.close()
        listOfNotNull(
          (inbox.mask or STREAM_OPEN).takeIf { !inbox.opened },
          inbox.mask or STREAM_DESTROY,
        )
      }
    }

  private suspend fun queueSignals(id: ULong, signals: () -> List<ULong>) {
    val queued = state.withLock {
      if (failure != null) return@withLock false
      val flags = signals()
      for (flag in flags) outgoing.addLast(encode(frame, Message.Stream(id, flag, null, null)))
      flags.isNotEmpty()
    }
    if (queued) flush()
  }

  private suspend fun acceptReply(id: ULong) {
    val accepted = state.withLock {
      val accepted = replyAcks.remove(id)
      if (accepted == null) earlyReplyAcks += id
      accepted
    }
    accepted?.complete(Unit)
  }

  private suspend fun respond(
    id: ULong,
    stream: ULong,
    data: ByteArray? = null,
    error: RemoteError? = null,
  ) {
    state.withLock {
      if (stream == 0uL) earlyReplyAcks.remove(id)
      outgoing.addLast(encode(frame, Message.Response(id, stream, data, error)))
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
 *   can be collected once, until the reply is sent or a streamed reply ends; a later collection
 *   ends empty. Stopping before its end, or not collecting it to the end in that time, tells a peer
 *   that has started streaming to stop sending.
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

private const val HIGH_WATER_MARK = 16 * 1024

private val DEFAULT_MAX_BUFFERED_BYTES: UInt = 16u * 1024u * 1024u

private class Inbox(val mask: ULong, private val maxBufferedBytes: UInt) {
  val chunks = Channel<ByteArray>(Channel.UNLIMITED)
  var opened = false
  private var buffered = 0L
  private var paused = false

  fun add(size: Int): Boolean {
    if (buffered + size > maxBufferedBytes.toLong()) throw StreamOverflowException()
    buffered += size
    if (paused || buffered < HIGH_WATER_MARK) return false
    paused = true
    return true
  }

  fun remove(size: Int): Boolean {
    buffered -= size
    if (!paused || buffered >= HIGH_WATER_MARK) return false
    paused = false
    return true
  }
}
