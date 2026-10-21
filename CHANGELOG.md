# Changelog

## 0.3.0

The frame codec is public, so a tool can encode and decode a single bare-rpc frame without a connection. Checked against the `bare-rpc-test` 0.1.0 wire fixtures, and against `bare-rpc` 1.3.8 running under Node.

- `Messages.encode` encodes a `Message` as one frame, and throws `IllegalArgumentException` for a stream frame that carries the error flag without an error.
- `Messages.decode` decodes the frame at the start of a buffer and returns the `Message` with the number of bytes the frame took. It throws `MalformedFrameException` or `UnknownMessageException` for a frame it rejects.
- `Message.Request`, `Message.Response` and `Message.Stream` describe a frame's body and compare their payloads by content. `RemoteError` is the error a peer sends in place of a reply or to close a stream.

## 0.2.0

Streams, in both directions, wire-compatible with the JavaScript `bare-rpc`. A stream frame no longer closes the connection. Still checked against the `hrpc-test` 0.2.4 wire fixtures, and against `bare-rpc` 1.3.8 running under Node.

- Breaking: `onRequest` and `CommandRouter` handlers return a `Reply`. `Reply.Data` is a single reply, and `Reply.Stream` streams one. To migrate, wrap each `ByteArray` reply in `Reply.Data`.
- `request(command, body)` streams a request body, and `IncomingRequest.body` reads the peer's as a `Flow`.
- `requestFlow` emits a streamed reply chunk by chunk, or a single reply as one chunk. `request` throws `UnexpectedStreamException` when the peer replies with a stream.
- The end that reads a request body or streams a reply needs the new `scope` parameter. Without one, the peer gets an `ERR_NO_SCOPE` error.
- Backpressure in both directions: a peer's pause holds this end's sends, and a stream pauses the peer once 16 KiB is buffered.
- `maxBufferedBytes`, 16 MiB by default, caps each stream. A peer that sends past it fails the connection with `StreamOverflowException`.
- Streams tear down as in JavaScript. Stopping a collection, or leaving a request body unread when its reply ends, tells the peer to stop sending, and the peer's destroy stops this end's sends. A body that throws closes its stream with the error, and the peer's close with an error fails the body or flow with a `RemoteException`. A request body stays readable until its reply ends, so it can be streamed back.
- A send that has started finishes even if its caller is cancelled, so frames queued behind it still go out.

## 0.1.0

Initial release.

- Kotlin Multiplatform library for JVM and Android (`minSdk` 29, Java 11 bytecode), wire-compatible with the JavaScript `bare-rpc`. Checked against the `hrpc-test` 0.2.4 wire fixtures, and against `bare-rpc` 1.3.8 running under Node.
- `BareRpc` over any transport: a `send` function for outgoing frames, plus `receive` for incoming bytes. It sends requests and events, and answers the peer's requests through `onRequest`. A peer's error arrives as a `RemoteException`.
- `CommandRouter` routes requests and events by command, and rejects an unknown request with `ERR_UNKNOWN_COMMAND`.
- A failure closes the connection, and so does `close()`. Pending requests resume with the cause, and later calls throw it. The causes are `MalformedFrameException`, `UnknownMessageException` and `ConnectionClosedException`.
- Frames are capped at `maxFrameSize`, 16 MiB by default.
- Streams are not supported yet. A stream frame closes the connection.
