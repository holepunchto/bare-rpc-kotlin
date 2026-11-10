package to.holepunch.bare.rpc

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import to.holepunch.compactEncoding.decode
import to.holepunch.compactEncoding.encode

/**
 * One end of a bare-rpc connection over a [Transport].
 *
 * The connection collects the data of [transport] in [scope] from the start. Their end closes it
 * with a [ConnectionClosedException], and a failure to collect them closes it with that failure. A
 * failure of [Transport.write] or of an event handler closes it too, while an exception from a
 * request handler goes back to the peer instead. A frame longer than [maxFrameSize] closes it with
 * a [FrameTooLargeException]. A frame that cannot be decoded closes it with a
 * [MalformedFrameException] or [UnknownMessageException], as does one that a valid peer never
 * sends. A frame for a request or stream that has finished, or that was never known, is ignored.
 * However the connection ends, it then closes [transport]: after writing the frames already queued
 * if it ended with a [ConnectionClosedException], and dropping them otherwise. Bodies still being
 * sent, of requests and of each [Reply.Stream], are cancelled with a [CancellationException] whose
 * cause is the exception the connection ended with.
 *
 * Each handler runs in a child of the connection's job, so a slow handler holds back no other
 * frame, and a handler may itself make requests to the peer.
 *
 * Cancelling a call before its request has finished cancels the request: the peer is told to stop
 * handling it, with the code `TIMEOUT` if the call timed out, as in `withTimeout`, and `CANCELLED`
 * otherwise. When the peer cancels a request, its handler is cancelled with a
 * [RequestCancelledException] that carries the peer's code, the bodies of the request and its reply
 * end as if the peer had destroyed them with the cancel's error, and the handler's answer is
 * dropped.
 *
 * @param transport the transport to the peer. A write is not cancelled once called, even when the
 *   caller that queued the frame is, since frames queued by other callers wait behind it.
 * @param scope the parent of the connection's work: collecting the data, running the handlers, and
 *   streaming the body of each request and each [Reply.Stream] all run in a child of its job, which
 *   [close] and any failure cancel. Cancelling it closes the connection, and a [Reply.Stream] that
 *   can no longer be streamed is answered with a [ConnectionClosedException] error instead.
 * @param handler answers the requests and receives the events of the peer.
 * @param maxFrameSize the longest frame accepted from the peer, length prefix included. A longer
 *   one fails the connection with a [FrameTooLargeException]. It must be positive.
 * @param maxBufferedBytes the most of one stream's data held for its collector. A peer that sends
 *   past it fails the connection with a [StreamOverflowException]. Below the 16 KiB pause mark, the
 *   peer is never paused first. It must be positive.
 * @throws IllegalArgumentException if [maxFrameSize] or [maxBufferedBytes] is not positive.
 */
public class Rpc(
  private val transport: Transport,
  scope: CoroutineScope,
  private val handler: Handler,
  maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
  private val maxBufferedBytes: Int = DEFAULT_MAX_BUFFERED_BYTES,
) {
  /**
   * Creates a connection that passes the requests and events of the peer to functions, as a
   * [Handler] would.
   *
   * @param transport the transport to the peer, as for the primary constructor.
   * @param scope the parent of the connection's work, as for the primary constructor.
   * @param onRequest answers each request the peer sends, as [Handler.handle] does. By default
   *   every request is rejected as an unknown command.
   * @param onEvent receives each event the peer sends. By default, events are dropped.
   * @param maxFrameSize the longest frame accepted from the peer, as for the primary constructor.
   * @param maxBufferedBytes the most of one stream's data held for its collector, as for the
   *   primary constructor.
   * @throws IllegalArgumentException if [maxFrameSize] or [maxBufferedBytes] is not positive.
   */
  public constructor(
    transport: Transport,
    scope: CoroutineScope,
    onRequest: suspend (IncomingRequest) -> Reply = { throw unknownCommand(it.command) },
    onEvent: suspend (IncomingEvent) -> Unit = {},
    maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
    maxBufferedBytes: Int = DEFAULT_MAX_BUFFERED_BYTES,
  ) : this(transport, scope, FunctionHandler(onRequest, onEvent), maxFrameSize, maxBufferedBytes)

  init {
    require(maxFrameSize > 0) { "maxFrameSize must be positive" }
    require(maxBufferedBytes > 0) { "maxBufferedBytes must be positive" }
  }

  private val job = SupervisorJob(scope.coroutineContext[Job])
  private val tasks = CoroutineScope(scope.coroutineContext + job)
  private val splitter = FrameSplitter(maxFrameSize)
  private val sending = Mutex()
  private val state = Mutex()
  private var lastId = 0uL
  private val pending = mutableMapOf<ULong, CompletableDeferred<ByteArray>>()
  private val requestIn = mutableMapOf<ULong, Inbox>()
  private val replyIn = mutableMapOf<ULong, Inbox>()
  private val requestOut = mutableMapOf<ULong, OutgoingBody>()
  private val replyOut = mutableMapOf<ULong, OutgoingBody>()
  private val earlyReplyAcks = mutableSetOf<ULong>()
  private val earlyReplyDestroys = mutableSetOf<ULong>()
  // The job of each request in progress, which runs its handler.
  private val liveRequests = mutableMapOf<ULong, Job>()
  private var lastRequestId = 0uL
  private val outgoing = ArrayDeque<ByteArray>()
  private var failure: Throwable? = null
  private val transportClosed = CompletableDeferred<Unit>()

  init {
    launchWithCleanup({ fail(ConnectionClosedException()) }) { read(transport.data) }
  }

  /**
   * Sends [data] to the peer as an event for [command], which gets no reply, and waits for it to be
   * written. Once the connection has closed, the event is dropped.
   */
  public suspend fun event(command: ULong, data: ByteArray) {
    try {
      sendMessage { Message.Request(0u, command, 0u, data) }
    } catch (e: Throwable) {
      if (e !== state.withLock { failure }) throw e
    }
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
   * stream, and returns the reply as soon as it comes. Once accepted, [body] is streamed to its end
   * in [scope] even after the reply, unless the peer stops reading it or the connection closes. A
   * reply before the peer accepts the stream skips [body] and closes the stream, with the reply's
   * error if it is one. If [body] throws, the stream is closed with the error, and this call throws
   * it if the reply has yet to come. Cancelling this call before the reply stops [body] and cancels
   * the request.
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
   * streamed reply, or a single reply as one chunk unless it is empty. Each collection sends a new
   * request. Cancelling a collection before the reply ends, or stopping it early as `first()` does,
   * cancels the request. A collector that fails instead tells a peer that has started streaming to
   * stop, with the error the collector failed with.
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
   * reply as one chunk unless it is empty. Once accepted, [body] is streamed to its end in [scope]
   * even after the reply ends, unless the peer stops reading it, the connection closes, or the
   * collection is cancelled or fails, which closes the stream. A single reply before the peer
   * accepts the stream closes it, with the reply's error if it is one. If [body] throws, the stream
   * is closed with the error, and the flow throws it if the reply has yet to end. Each collection
   * sends a new request. Cancelling a collection before the reply ends, or stopping it early as
   * `first()` does, cancels the request. A collector that fails instead tells a peer that has
   * started streaming to stop, with the error the collector failed with.
   *
   * @throws RemoteException if the peer replies or closes the stream with an error.
   * @throws Throwable the failure that closed the connection, a [ConnectionClosedException] after
   *   [close].
   */
  public fun requestFlow(command: ULong, body: Flow<ByteArray>): Flow<ByteArray> =
    replyFlow(command, null, body)

  /**
   * Closes the connection: pending requests and every later call throw a
   * [ConnectionClosedException], and the rest of the data received from the peer goes undelivered.
   * Closing after a failure keeps that failure. It stops collecting the data, cancels running
   * handlers and the bodies being streamed, writes the frames already queued, and closes the
   * transport. It waits for all of that to finish, unless it is called from a handler or a body.
   * Calling it again does nothing more.
   */
  public suspend fun close() {
    fail(ConnectionClosedException())
    // A caller inside the connection's own work would wait on itself.
    val caller = currentCoroutineContext()[Job]
    if (caller == null || !job.runs(caller)) job.join()
  }

  /**
   * Waits for the connection to close and its transport with it. It returns if the connection
   * closed cleanly, by the end of the data or by [close], and throws the failure otherwise.
   *
   * @throws Throwable the failure that closed the connection, if it was not a
   *   [ConnectionClosedException].
   */
  public suspend fun wait() {
    transportClosed.await()
    val cause = state.withLock { failure }
    if (cause !is ConnectionClosedException) throw checkNotNull(cause)
  }

  private fun Job.runs(other: Job): Boolean = this == other || children.any { it.runs(other) }

  // The number of entries kept per request or stream, so tests can see that none are left behind.
  internal suspend fun tracked(): Int = state.withLock {
    listOf(pending, requestIn, replyIn, requestOut, replyOut, liveRequests).sumOf { it.size } +
      listOf(earlyReplyAcks, earlyReplyDestroys).sumOf { it.size }
  }

  private suspend fun read(data: Flow<ByteArray>) {
    try {
      data.collect { receive(it) }
      if (splitter.hasPartialFrame) fail(MalformedFrameException("Truncated frame"))
    } catch (e: Throwable) {
      if (e is CancellationException) throw e
      fail(e)
    }
  }

  // Replies to requests never wait for a write in progress.
  private suspend fun receive(bytes: ByteArray) {
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

  private suspend fun sendRequest(
    command: ULong,
    data: ByteArray?,
    body: Flow<ByteArray>?,
  ): ByteArray {
    // Taking a free lock does not check for cancellation, so a cancelled call would send its
    // request only to cancel it at once.
    currentCoroutineContext().ensureActive()
    val reply = CompletableDeferred<ByteArray>()
    val accepted = CompletableDeferred<Boolean>()
    val paused = MutableStateFlow(false)
    var id = 0uL
    var sender: Job? = null
    try {
      sendMessage {
        id = ++lastId
        pending[id] = reply
        if (body != null) requestOut[id] = OutgoingBody(accepted, paused)
        Message.Request(id, command, if (body != null) STREAM_OPEN else 0u, data)
      }
      if (body != null) {
        sender = launchRequestBody(id, body, accepted, paused) { reply.completeExceptionally(it) }
      }
      return reply.await()
    } catch (e: CancellationException) {
      // A body can throw a cancellation of its own, which fails the reply but not the caller.
      if (!currentCoroutineContext().isActive) cancelRequest(id, e)
      sender?.cancel()
      throw e
    } finally {
      withContext(NonCancellable) {
        state.withLock {
          pending.remove(id)
          if (sender == null) requestOut.remove(id)
        }
      }
    }
  }

  private fun replyFlow(
    command: ULong,
    data: ByteArray?,
    body: Flow<ByteArray>?,
  ): Flow<ByteArray> = flow {
    currentCoroutineContext().ensureActive()
    val inbox = Inbox(STREAM_RESPONSE, maxBufferedBytes)
    val accepted = CompletableDeferred<Boolean>()
    val paused = MutableStateFlow(false)
    var id = 0uL
    var failed: Throwable? = null
    var sender: Job? = null
    try {
      sendMessage {
        id = ++lastId
        replyIn[id] = inbox
        if (body != null) requestOut[id] = OutgoingBody(accepted, paused)
        Message.Request(id, command, if (body != null) STREAM_OPEN else 0u, data)
      }
      sendMessage {
        inbox.isOpened = true
        Message.Stream(id, STREAM_RESPONSE or STREAM_OPEN, null, null)
      }
      if (body != null) {
        sender = launchRequestBody(id, body, accepted, paused) { inbox.chunks.close(it) }
      }
      for (chunk in inbox.chunks) {
        take(id, replyIn, inbox, chunk)
        try {
          emit(chunk)
        } catch (e: Throwable) {
          failed = e
          throw e
        }
      }
    } catch (e: Throwable) {
      // A body can throw a cancellation of its own, which fails the reply but not the collector.
      if (e is CancellationException && (e === failed || !currentCoroutineContext().isActive)) {
        cancelRequest(id, e)
      }
      if (e is CancellationException || e === failed) sender?.cancel()
      throw e
    } finally {
      stopReading(id, replyIn, failed)
      if (sender == null) withContext(NonCancellable) { state.withLock { requestOut.remove(id) } }
    }
  }

  // A request whose reply has yet to end is cancelled at the peer, and what is left of it here is
  // dropped without stream frames, as the peer drops its side too.
  private suspend fun cancelRequest(id: ULong, cause: CancellationException) =
    withContext(NonCancellable) {
      val error = if (cause is TimeoutCancellationException) TIMED_OUT else CANCELLED
      val inbox = state.withLock {
        if (failure != null || (id !in pending && id !in replyIn)) return@withContext
        pending.remove(id)
        requestOut.remove(id)
        outgoing.addLast(encode(frame, Message.Cancel(id, error)))
        replyIn.remove(id)
      }
      inbox?.chunks?.close()
      flush()
    }

  // Runs in the connection's scope, as the body streams on to its end after the reply comes.
  private fun launchRequestBody(
    id: ULong,
    body: Flow<ByteArray>,
    accepted: CompletableDeferred<Boolean>,
    paused: MutableStateFlow<Boolean>,
    onFailure: (Throwable) -> Unit,
  ): Job =
    launchWithCleanup({
      // A body stopped before its end leaves the peer's stream open.
      val open = state.withLock { requestOut.remove(id)?.isDone == false }
      if (open) queueSignals(id) { listOf(STREAM_REQUEST or STREAM_CLOSE) }
    }) {
      val started = state.withLock {
        val out = requestOut[id]?.takeUnless { it.isDone }
        out?.sender = currentCoroutineContext().job
        out != null
      }
      if (!started || !accepted.await()) return@launchWithCleanup
      sendBody(id, STREAM_REQUEST, body, paused) { requestOut[id]?.isDone = true }?.let(onFailure)
    }

  private suspend fun sendBody(
    id: ULong,
    mask: ULong,
    body: Flow<ByteArray>,
    paused: MutableStateFlow<Boolean>,
    onEnd: () -> Unit = {},
  ): Throwable? {
    // A cancel can drop the body between the sender's last check for cancellation and its frame.
    val outs = if (mask == STREAM_REQUEST) requestOut else replyOut
    var error: Throwable? = null
    body
      .catch { error = it }
      .collect {
        paused.awaitResume()
        sendMessage { Message.Stream(id, mask or STREAM_DATA, it, null).takeIf { id in outs } }
      }
    val failed = error
    paused.awaitResume()
    sendMessage {
      onEnd()
      if (id !in outs) null
      else if (failed == null) Message.Stream(id, mask or STREAM_END, null, null)
      else Message.Stream(id, mask or STREAM_CLOSE or STREAM_ERROR, null, remoteError(failed))
    }
    return failed
  }

  private suspend fun MutableStateFlow<Boolean>.awaitResume() {
    first { !it }
  }

  private suspend fun sendMessage(message: () -> Message?) {
    state.withLock {
      failure?.let { throw it }
      outgoing.addLast(encode(frame, message() ?: return))
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
        transport.write(next)
      } catch (e: Throwable) {
        fail(e)
        return e
      }
    }
  }

  private suspend fun flush() {
    while (state.withLock { failure == null && outgoing.isNotEmpty() } && sending.tryLock()) {
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
    val senders: List<Job>
    state.withLock {
      if (failure != null) return
      failure = cause
      if (cause !is ConnectionClosedException) outgoing.clear()
      val outs = requestOut.values + replyOut.values
      waiting = pending.values + outs.mapNotNull { it.accepted }
      bodies = (requestIn.values + replyIn.values).map { it.chunks }
      pauses = outs.map { it.paused }
      senders = outs.mapNotNull { it.sender }
      listOf(pending, requestIn, replyIn, requestOut, replyOut, liveRequests).forEach { it.clear() }
      listOf(earlyReplyAcks, earlyReplyDestroys).forEach { it.clear() }
    }
    waiting.forEach { it.completeExceptionally(cause) }
    pauses.forEach { it.value = false }
    bodies.forEach { it.close(cause) }
    val stop = CancellationException("Connection closed", cause)
    senders.forEach { it.cancel(stop) }
    closeTransport()
    job.cancel(stop)
  }

  @OptIn(DelicateCoroutinesApi::class)
  private fun closeTransport() =
    // Starts even on a cancelled scope, so the transport is always closed.
    tasks.launch(start = CoroutineStart.ATOMIC) {
      withContext(NonCancellable) {
        sending.withLock {
          while (true) {
            val next = state.withLock { outgoing.removeFirstOrNull() } ?: break
            try {
              transport.write(next)
            } catch (e: Throwable) {
              break
            }
          }
        }
        try {
          transport.close()
        } catch (e: Throwable) {} finally {
          transportClosed.complete(Unit)
        }
      }
    }

  private suspend fun dispatch(message: Message) {
    violation(message)?.let { throw MalformedFrameException(it) }
    when (message) {
      is Message.Request ->
        if (message.id != 0uL) answer(message)
        else receiveEvent(IncomingEvent(message.command, message.data!!))
      is Message.Response -> {
        val error = message.error
        if (error == null && message.stream != 0uL) {
          val (reply, accepted) =
            state.withLock {
              if (message.id in replyIn) return
              pending.remove(message.id) to refuseRequestBody(message.id)
            }
          if (reply == null && accepted == null) return
          val exception = UnexpectedStreamException()
          accepted?.complete(false)
          // The peer's writer waits for an ack that will never come, so its stream is destroyed.
          queueSignals(
            message.id,
            RemoteError(exception.message.orEmpty(), "UNEXPECTED_STREAM", 0),
          ) {
            listOfNotNull(
              (STREAM_RESPONSE or STREAM_DESTROY).takeIf { reply != null },
              (STREAM_REQUEST or STREAM_CLOSE or STREAM_ERROR).takeIf { accepted != null },
            )
          }
          reply?.completeExceptionally(exception)
          return
        }
        val reply: CompletableDeferred<ByteArray>?
        val accepted: CompletableDeferred<Boolean>?
        val chunks: Channel<ByteArray>?
        state.withLock {
          reply = pending.remove(message.id)
          accepted = refuseRequestBody(message.id)
          chunks = replyIn.remove(message.id)?.chunks
        }
        if (accepted != null) closeRefused(message.id, accepted, error)
        if (error != null) {
          val exception = remoteException(error)
          reply?.completeExceptionally(exception)
          chunks?.close(exception)
        } else {
          val data = message.data!!
          reply?.complete(data)
          if (data.isNotEmpty()) chunks?.trySend(data)
          chunks?.close()
        }
      }
      is Message.Stream -> {
        val event = message.flags and (STREAM_REQUEST or STREAM_RESPONSE or STREAM_ERROR).inv()
        if (message.flags and STREAM_REQUEST != 0uL) onRequestStream(message, event)
        else onReplyStream(message, event)
      }
      is Message.Cancel -> onCancel(message)
    }
  }

  private suspend fun onCancel(message: Message.Cancel) {
    val error = message.error
    val exception = RequestCancelledException(error.message, error.code, error.errno)
    val (job, inbox, out) =
      state.withLock {
        val job = liveRequests.remove(message.id)
        val inbox = requestIn.remove(message.id)
        if (job == null && inbox == null) return
        earlyReplyAcks.remove(message.id)
        earlyReplyDestroys.remove(message.id)
        Triple(job, inbox, replyOut.remove(message.id))
      }
    inbox?.chunks?.close(remoteException(error))
    out?.takeSender()?.cancel(exception)
    job?.cancel(exception)
  }

  // Why a valid peer would never send this frame, or null if it could.
  private fun violation(message: Message): String? =
    when (message) {
      is Message.Request ->
        when {
          message.stream == 0uL -> null
          message.id == 0uL -> "an event cannot open a stream"
          message.stream != STREAM_OPEN -> "a request cannot have stream flags ${message.stream}"
          else -> null
        }
      is Message.Response ->
        when {
          message.id == 0uL -> "a response needs a request id"
          message.stream == 0uL -> null
          message.error != null || message.stream != STREAM_OPEN ->
            "a response cannot have stream flags ${message.stream}"
          else -> null
        }
      is Message.Stream ->
        when {
          message.id == 0uL -> "a stream frame needs a request id"
          message.flags !in streamFlags -> "a stream frame cannot have flags ${message.flags}"
          else -> null
        }
      is Message.Cancel -> if (message.id == 0uL) "a cancel needs a request id" else null
    }

  private suspend fun onRequestStream(message: Message.Stream, event: ULong) {
    when {
      event == STREAM_OPEN ->
        state.withLock { requestOut[message.id]?.takeAccepted() }?.complete(true)
      event == STREAM_PAUSE || event == STREAM_RESUME ->
        state.withLock { requestOut[message.id] }?.paused?.value = event == STREAM_PAUSE
      event == STREAM_DATA ->
        state.withLock { requestIn[message.id] }?.let { deliver(message.id, it, message.data!!) }
      event == STREAM_END -> state.withLock { requestIn.remove(message.id) }?.chunks?.close()
      event == STREAM_CLOSE ->
        state
          .withLock { requestIn.remove(message.id) }
          ?.chunks
          ?.close(message.error?.let(::remoteException))
      event == STREAM_DESTROY -> destroy(requestOut, message.id)
    }
  }

  // Takes the ack of a request body that a reply refuses. The caller closes the body itself, so its
  // sender has nothing to close.
  private fun refuseRequestBody(id: ULong): CompletableDeferred<Boolean>? {
    val out = requestOut[id] ?: return null
    return out.takeAccepted()?.also { out.isDone = true }
  }

  // A request body the peer never accepted will never be read, so its stream is closed.
  private suspend fun closeRefused(
    id: ULong,
    accepted: CompletableDeferred<Boolean>,
    error: RemoteError?,
  ) {
    accepted.complete(false)
    queueSignals(id, error) {
      listOf(STREAM_REQUEST or STREAM_CLOSE or if (error != null) STREAM_ERROR else 0uL)
    }
  }

  // The peer can destroy a body before its sender has started, and a reply stream it opened early
  // before the reply that streams it is sent.
  private suspend fun destroy(outs: Map<ULong, OutgoingBody>, id: ULong, early: () -> Unit = {}) {
    val sender = state.withLock {
      val out = outs[id]
      out?.isDone = true
      val sender = out?.takeSender()
      if (sender == null) early()
      sender
    }
    sender?.cancel()
  }

  private suspend fun onReplyStream(message: Message.Stream, event: ULong) {
    when {
      event == STREAM_OPEN -> acceptReply(message.id)
      event == STREAM_PAUSE || event == STREAM_RESUME ->
        state.withLock { replyOut[message.id] }?.paused?.value = event == STREAM_PAUSE
      event == STREAM_DATA ->
        state.withLock { replyIn[message.id] }?.let { deliver(message.id, it, message.data!!) }
      event == STREAM_END || event == STREAM_CLOSE -> {
        val error = message.error
        val (chunks, accepted) =
          state.withLock { replyIn.remove(message.id)?.chunks to refuseRequestBody(message.id) }
        if (accepted != null) closeRefused(message.id, accepted, error)
        chunks?.close(error?.let(::remoteException))
      }
      event == STREAM_DESTROY ->
        destroy(replyOut, message.id) {
          if (earlyReplyAcks.remove(message.id)) earlyReplyDestroys += message.id
        }
    }
  }

  private suspend fun answer(request: Message.Request) {
    val inbox = if (request.stream != 0uL) Inbox(STREAM_REQUEST, maxBufferedBytes) else null
    // Kept with the request before the handler starts, so that a cancel always finds it.
    val handling = state.withLock {
      if (request.id in liveRequests) {
        throw MalformedFrameException("request ${request.id} is still in progress")
      }
      lastRequestId = maxOf(lastRequestId, request.id)
      failure?.let { throw it }
      if (inbox != null) requestIn[request.id] = inbox
      Job(job).also { liveRequests[request.id] = it }
    }
    if (inbox == null) {
      val incoming = IncomingRequest(request.command, request.data!!, null)
      launchWithCleanup({}, CoroutineStart.UNDISPATCHED, handling) {
        reply(request.id) { handler.handle(incoming) }
      }
    } else {
      val incoming = IncomingRequest(request.command, ByteArray(0), requestBody(request.id, inbox))
      launchWithCleanup({ stopReading(request.id, requestIn) }, context = handling) {
        reply(request.id) { handler.handle(incoming) }?.join()
      }
    }
    handling.complete()
  }

  private fun requestBody(id: ULong, inbox: Inbox): Flow<ByteArray> {
    val chunks =
      inbox.chunks
        .consumeAsFlow()
        .onStart {
          queueSignals(id) {
            if (requestIn[id] !== inbox) return@queueSignals emptyList()
            inbox.isOpened = true
            listOf(STREAM_REQUEST or STREAM_OPEN)
          }
        }
        .onEach { take(id, requestIn, inbox, it) }
        .onCompletion { stopReading(id, requestIn, it) }
    return flow {
      state.withLock {
        if (inbox.isCollected) throw AlreadyReceivedException()
        inbox.isCollected = true
      }
      emitAll(chunks)
    }
  }

  // A request stays live until its reply is sent, whole or streamed. One whose handler never
  // replies is cancelled only when the connection fails, which forgets it.
  private suspend fun reply(id: ULong, handler: suspend () -> Reply): Job? {
    val reply =
      try {
        handler()
      } catch (e: Throwable) {
        if (e is CancellationException) currentCoroutineContext().ensureActive()
        respond(id, error = remoteError(e))
        return null
      }
    return when (reply) {
      is Reply.Data -> {
        respond(id, data = reply.data)
        null
      }
      is Reply.Stream -> streamReply(id, reply.body)
    }
  }

  // Starts inline, so handlers begin in the order of their frames, but the read goes on once one
  // suspends.
  private fun receiveEvent(event: IncomingEvent) =
    launchWithCleanup({}, CoroutineStart.UNDISPATCHED) {
      try {
        handler.handle(event)
      } catch (e: Throwable) {
        if (e is CancellationException) currentCoroutineContext().ensureActive()
        fail(e)
      }
    }

  private suspend fun streamReply(id: ULong, body: Flow<ByteArray>): Job? {
    val accepted = CompletableDeferred<Boolean>()
    val paused = MutableStateFlow(false)
    val sender =
      launchWithCleanup({
        state.withLock {
          replyOut.remove(id)
          liveRequests.remove(id)
        }
      }) {
        accepted.await()
        sendBody(id, STREAM_RESPONSE, body, paused)
      }
    // Taking the lock can throw a cancellation, so a sender is cancelled unless it was kept.
    var kept = false
    val running =
      try {
        state.withLock {
          failure?.let { throw it }
          // A sender launched on a cancelled scope never runs, so the reply fails instead.
          if (sender.isCancelled) return@withLock false
          // The peer has cancelled the request, so nothing is streamed.
          if (id !in liveRequests) return null
          // Queued before the sender can start, so the reply goes out before its body ends.
          outgoing.addLast(encode(frame, Message.Response(id, STREAM_OPEN, null, null)))
          if (!earlyReplyDestroys.remove(id)) {
            val early = earlyReplyAcks.remove(id)
            if (early) accepted.complete(true)
            replyOut[id] =
              OutgoingBody(accepted.takeUnless { early }, paused).also { it.sender = sender }
            kept = true
          }
          true
        }
      } finally {
        if (!kept) sender.cancel()
      }
    if (!running) {
      respond(id, error = remoteError(ConnectionClosedException()))
      return null
    }
    flush()
    return sender
  }

  @OptIn(DelicateCoroutinesApi::class)
  private fun launchWithCleanup(
    cleanup: suspend () -> Unit,
    // Either start runs even on a cancelled scope, so cleanup always runs.
    start: CoroutineStart = CoroutineStart.ATOMIC,
    context: CoroutineContext = EmptyCoroutineContext,
    block: suspend () -> Unit,
  ) =
    tasks.launch(context, start) {
      try {
        currentCoroutineContext().ensureActive()
        block()
      } catch (e: Throwable) {
        // Every error here is the connection's failure, which every call already throws.
        if (e is CancellationException) currentCoroutineContext().ensureActive()
      } finally {
        withContext(NonCancellable) { cleanup() }
      }
    }

  private suspend fun deliver(id: ULong, inbox: Inbox, data: ByteArray) {
    queueSignals(id) { listOfNotNull((inbox.mask or STREAM_PAUSE).takeIf { inbox.add(data.size) }) }
    inbox.chunks.trySend(data)
  }

  // A stream that has ended or been cancelled still hands out what it buffered, without signals.
  private suspend fun take(id: ULong, inboxes: Map<ULong, Inbox>, inbox: Inbox, data: ByteArray) {
    queueSignals(id) {
      if (inboxes[id] !== inbox) return@queueSignals emptyList()
      listOfNotNull((inbox.mask or STREAM_RESUME).takeIf { inbox.remove(data.size) })
    }
  }

  // A collector that failed destroys the stream with its error, and a cancelled one without.
  private suspend fun stopReading(
    id: ULong,
    inboxes: MutableMap<ULong, Inbox>,
    cause: Throwable? = null,
  ) =
    withContext(NonCancellable) {
      val error = cause?.takeUnless { it is CancellationException }?.let(::remoteError)
      queueSignals(id, error) {
        val inbox = inboxes.remove(id) ?: return@queueSignals emptyList()
        inbox.chunks.close()
        listOfNotNull(
          (inbox.mask or STREAM_OPEN).takeIf { !inbox.isOpened },
          inbox.mask or STREAM_DESTROY or if (error != null) STREAM_ERROR else 0uL,
        )
      }
    }

  private suspend fun queueSignals(
    id: ULong,
    error: RemoteError? = null,
    signals: () -> List<ULong>,
  ) {
    val queued = state.withLock {
      if (failure != null) return@withLock false
      val flags = signals()
      for (flag in flags) {
        val carried = if (flag and STREAM_ERROR != 0uL) error else null
        outgoing.addLast(encode(frame, Message.Stream(id, flag, null, carried)))
      }
      flags.isNotEmpty()
    }
    if (queued) flush()
  }

  private suspend fun acceptReply(id: ULong) {
    val accepted = state.withLock {
      val accepted = replyOut[id]?.takeAccepted()
      if (accepted == null && canAnswer(id) && earlyReplyAcks.add(id)) {
        // Dropping an ack a valid peer sent would leave its reply waiting with nothing to show why.
        if (earlyReplyAcks.size + earlyReplyDestroys.size > MAX_EARLY_REPLY_ACKS) {
          throw MalformedFrameException("too many acks for replies not yet sent")
        }
      }
      accepted
    }
    accepted?.complete(true)
  }

  // The peer numbers its requests in order and may ack a reply before it sends the request, so an
  // ack is kept for a request in progress or one it has yet to send.
  private fun canAnswer(id: ULong) = id in liveRequests || id > lastRequestId

  private suspend fun respond(
    id: ULong,
    data: ByteArray? = null,
    error: RemoteError? = null,
  ) {
    state.withLock {
      // A request the peer has cancelled is no longer live, so its answer is dropped.
      if (failure != null || id !in liveRequests) return
      liveRequests.remove(id)
      earlyReplyAcks.remove(id)
      earlyReplyDestroys.remove(id)
      outgoing.addLast(encode(frame, Message.Response(id, 0u, data, error)))
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
 * @property body the request's streamed payload, or null. Collecting it accepts the stream. It can
 *   be collected only once, and a second collection throws an [AlreadyReceivedException]. A
 *   collection that starts after the reply is sent, or after a streamed reply ends, ends empty.
 *   Stopping before its end, or not collecting it to the end in that time, tells a peer that has
 *   started streaming to stop sending, with the error the collector failed with, if any.
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

private val CANCELLED = RemoteError("Request was cancelled", "CANCELLED", 0)

private val TIMED_OUT = RemoteError("Request timed out", "TIMEOUT", 0)

private const val HIGH_WATER_MARK = 16 * 1024

private const val MAX_EARLY_REPLY_ACKS = 1024

private const val DEFAULT_MAX_BUFFERED_BYTES: Int = 16 * 1024 * 1024

private class OutgoingBody(
  var accepted: CompletableDeferred<Boolean>?,
  val paused: MutableStateFlow<Boolean>,
) {
  var sender: Job? = null
  var isDone = false

  fun takeAccepted() = accepted.also { accepted = null }

  fun takeSender() = sender.also { sender = null }
}

private class Inbox(val mask: ULong, private val maxBufferedBytes: Int) {
  val chunks = Channel<ByteArray>(Channel.UNLIMITED)
  var isOpened = false
  var isCollected = false
  private var buffered = 0L
  private var isPaused = false

  fun add(size: Int): Boolean {
    if (buffered + size > maxBufferedBytes) throw StreamOverflowException()
    buffered += size
    if (isPaused || buffered < HIGH_WATER_MARK) return false
    isPaused = true
    return true
  }

  fun remove(size: Int): Boolean {
    buffered -= size
    if (!isPaused || buffered >= HIGH_WATER_MARK) return false
    isPaused = false
    return true
  }
}
