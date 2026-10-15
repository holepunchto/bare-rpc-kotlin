# Changelog

## 0.1.0

Initial release.

- Kotlin Multiplatform library for JVM and Android (`minSdk` 29, Java 11 bytecode), wire-compatible with the JavaScript `bare-rpc`. Checked against the `hrpc-test` 0.2.4 wire fixtures, and against `bare-rpc` 1.3.8 running under Node.
- `BareRpc` over any transport: a `send` function for outgoing frames, plus `receive` for incoming bytes. It sends requests and events, and answers the peer's requests through `onRequest`. A peer's error arrives as a `RemoteException`.
- `CommandRouter` routes requests and events by command, and rejects an unknown request with `ERR_UNKNOWN_COMMAND`.
- A failure closes the connection, and so does `close()`. Pending requests resume with the cause, and later calls throw it. The causes are `MalformedFrameException`, `UnknownMessageException` and `ConnectionClosedException`.
- Frames are capped at `maxFrameSize`, 16 MiB by default.
- Streams are not supported yet. A stream frame closes the connection.
