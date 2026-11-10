# bare-rpc-kotlin

RPC over any byte transport for Kotlin. The Kotlin counterpart of [bare-rpc](https://github.com/holepunchto/bare-rpc), and wire compatible with it. Built on [compact-encoding-kotlin](https://github.com/holepunchto/compact-encoding-kotlin).

```groovy
repositories {
  maven { url "https://jitpack.io" }
}

dependencies {
  implementation "com.github.holepunchto.bare-rpc-kotlin:bare-rpc-kotlin:<tag>"
}
```

## Usage

This example wires two peers together in-process and makes one request:

```kotlin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.consumeAsFlow
import to.holepunch.bare.rpc.Reply
import to.holepunch.bare.rpc.Rpc
import to.holepunch.bare.rpc.Transport

suspend fun main() = coroutineScope {
  val toServer = Channel<ByteArray>(Channel.UNLIMITED)
  val toClient = Channel<ByteArray>(Channel.UNLIMITED)

  val server =
    Rpc(
      Transport(toServer.consumeAsFlow(), toClient::send) { toClient.close() },
      this,
      onRequest = { Reply.Data("pong".encodeToByteArray()) },
    )

  val client = Rpc(Transport(toClient.consumeAsFlow(), toServer::send) { toServer.close() }, this)

  val reply = client.request(1u, "ping".encodeToByteArray())
  println(reply.decodeToString()) // pong

  client.close()
  server.close()
}
```

`Rpc` runs a session over a `Transport`, which has three parts: `data`, a `Flow` of the bytes that arrive from the peer, `write(data)`, which writes one whole frame to the peer, and `close()`. `write` may suspend for backpressure, but must not wait on `data` being collected. The `Transport(data, write, close)` function builds one from its parts, such as for the IPC of a worklet from [bare-kit-kotlin](https://github.com/holepunchto/bare-kit-kotlin):

```kotlin
val rpc = Rpc(Transport(ipc.data, ipc::write, ipc::close), scope, handler = router)
```

`Rpc` collects `data` in `scope`, which also runs the handlers and the streams.

## Command routing

A `CommandRouter` sends each request and event to the handler registered for its command. Pass it to `Rpc` as its `handler`:

```kotlin
val router = CommandRouter()

router.onRequest(1u) { Reply.Data("pong".encodeToByteArray()) }
router.onEvent(2u) { event -> println(event.command) }

val rpc = Rpc(transport, scope, handler = router)
```

A request handler returns the `Reply` to answer with. Register every handler before you create the `Rpc`. A handler replaces any handler registered before it for the same command. A request for a command with no handler is rejected with the code `UNKNOWN_COMMAND`. An event for a command with no handler is dropped.

`CommandRouter` is a `Handler`. You can write your own `Handler` instead, with a `handle` function for requests and one for events. By default, the first rejects requests with the code `UNKNOWN_COMMAND` and the second drops events.

## Requests and responses

`request(command, data)` sends a request and returns its reply. `event(command, data)` sends an event, which gets no reply, and waits for it to be written. Each request and event of the peer is handled in its own coroutine in `scope`, so a slow handler does not hold back the others.

A request handler answers by returning a `Reply`. `Reply.Data` sends a payload at once, and `Reply.Stream` streams one. So each request gets exactly one answer.

If a request handler throws, the request is rejected with the exception. A `RemoteException` is sent with its message, code and errno. Any other exception is sent with its message, an empty code and errno 0.

When the peer rejects a request, `request()` throws a `RemoteException` with the `message`, `code` and `errno` of the peer.

Without `onRequest`, every request is rejected with the code `UNKNOWN_COMMAND`. Without `onEvent`, events are dropped.

## Streaming requests and responses

A request can stream its body, its reply, or both:

- `request(command, body)` streams the body from a `Flow` and returns a single reply.
- `requestFlow(command, data)` sends a plain request and returns a `Flow` of the reply.
- `requestFlow(command, body)` streams the body and returns a `Flow` of the reply.

Each collection of a flow from `requestFlow` sends a new request. On the answering side, `request.body` is the `Flow` of the body, or null if the request has none, and a `Reply.Stream` streams the reply. This handler echoes the body back:

```kotlin
router.onRequest(1u) { request -> Reply.Stream(request.body!!) }

val body = flowOf("ping".encodeToByteArray())

rpc.requestFlow(1u, body).collect { println(it.decodeToString()) } // ping
```

A stream only carries data once both ends have opened it, so a body waits until the peer has opened its end. Collecting `request.body` opens the body, and collecting the flow from `requestFlow` opens the reply. If the peer answers before it opens the body, the body is not sent. If a body you send throws, its stream closes with that error. A stream that the peer closes with an error throws a `RemoteException`.

Once 16 KiB of one stream is buffered and not yet collected, the peer is asked to pause. It is asked to resume once the collector drains the buffer below that. A body you send waits while the peer has paused it. If the peer keeps sending anyway, a stream holds at most `maxBufferedBytes`, 16 MiB by default, before the session fails with a `StreamOverflowException`.

The body of a request can be collected once. A second collection throws an `AlreadyReceivedException`.

## Cancellation

Cancelling the coroutine of a call before its answer has arrived cancels the request. This covers `request()`, and the collection of a flow from `requestFlow()` until the reply ends, including one that stops early as `first()` does. The peer is told to stop handling the request, and its bodies are dropped on both ends. If the call timed out in `withTimeout`, the peer gets the code `TIMEOUT`, and `CANCELLED` otherwise:

```kotlin
val reply = withTimeout(5.seconds) { rpc.request(1u, "ping".encodeToByteArray()) }
```

Once the answer has arrived, cancelling does nothing, and a body that is still being sent keeps streaming to its end, as described under [Streaming requests and responses](#streaming-requests-and-responses). A call whose coroutine is already cancelled sends no request at all.

On the answering side, a cancel of the peer cancels the coroutine of the handler with a `RequestCancelledException`. It is a `CancellationException` that carries the `message`, `code` and `errno` of the peer. The bodies of the request end as if the peer had destroyed them with that error: a collection of `request.body` throws a `RemoteException` with it, and a `Reply.Stream` stops. The answer of the handler is dropped:

```kotlin
router.onRequest(1u) { request ->
  try {
    Reply.Data(search(request.data)) // Suspends until the peer cancels
  } catch (e: RequestCancelledException) {
    println("The peer cancelled with ${e.code}")
    throw e
  }
}
```

When the session ends, the request and event handlers that are still running are cancelled too.

Cancelling a request needs a peer that supports it. A peer without support fails the session with an `UnknownMessageException` when it receives a cancel.

## Termination

The session ends with a `ConnectionClosedException` when you call `close()`, `scope` is cancelled, or `data` ends. It fails with an exception instead when:

- `data` ends partway through a frame, with a `MalformedFrameException`.
- Collecting `data` or writing to the transport throws, with that exception.
- An event handler throws, with that exception.
- The peer sends a bad frame, as described under [Untrusted peers](#untrusted-peers).

Pending requests and streams then throw the exception, and so do later requests. Bodies that are still being sent are cancelled, with the exception as the cause. Events sent after the end are dropped.

The session then stops collecting `data`, cancels running request and event handlers, and closes the transport. If it ended with a `ConnectionClosedException`, it first writes the frames already queued. Otherwise it drops them.

`close()` waits for the transport to close, unless you call it from a handler or a body. A handler that calls `close()` itself is cancelled once `close()` returns, at its next suspension point. `wait()` waits for the session to end and the transport to close. It returns if the session ended with a `ConnectionClosedException`, and throws the exception otherwise:

```kotlin
try {
  rpc.wait()
} catch (e: Exception) {
  println("The session failed: $e")
}
```

## Untrusted peers

`Rpc` does not trust the peer to send valid frames. If a frame is longer than `maxFrameSize`, cannot be decoded, or breaks the protocol, the session fails with an `RpcException`. The type tells you why:

- `FrameTooLargeException`: The frame, with its length prefix, is longer than `maxFrameSize`. This is checked as soon as the length prefix arrives, before the rest of the frame is read.
- `UnknownMessageException`: The frame has a message type that `Rpc` does not know.
- `MalformedFrameException`: The frame cannot be decoded, `data` ends partway through a frame, or the frame is one that a valid peer never sends. Examples are a response, stream or cancel frame with an id of 0, a stream frame with an invalid set of flags, and a new request that reuses the id of a request that is still in progress.
- `StreamOverflowException`: A stream holds more than `maxBufferedBytes` bytes that have not been collected. This only happens when the peer keeps sending after it was asked to pause.

A frame that refers to a request or stream that has already finished, or that was never known, is ignored. The same goes for frames that arrive for a stream that has already ended or been destroyed.

If you read the reply as a stream with `requestFlow()` but the peer replies without one, the reply arrives as a single chunk, unless it is empty, and the flow then ends. An error reply makes the flow throw a `RemoteException`.

If you expect a plain reply with `request()` but the peer replies with a stream, the call throws an `UnexpectedStreamException`. The session destroys the stream toward the peer and stays open.

## License

Apache-2.0
