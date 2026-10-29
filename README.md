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

```kotlin
import to.holepunch.bare.rpc.CommandRouter
import to.holepunch.bare.rpc.Reply
import to.holepunch.bare.rpc.Rpc

val router = CommandRouter()

router.onRequest(1u) { request ->
  Reply.Data("hello ${request.data.decodeToString()}".encodeToByteArray())
}

val rpc =
  Rpc(
    transport.data,
    transport::write,
    scope,
    onRequest = router::dispatch,
    onEvent = router::dispatch,
  )

val reply = rpc.request(1u, "ada".encodeToByteArray())
```

Any transport with a `Flow` of `ByteArray` to read and a suspending function to write can be used, such as the IPC of a worklet from [bare-kit-kotlin](https://github.com/holepunchto/bare-kit-kotlin). `Rpc` collects the data in `scope`, which also runs the streams. The connection closes when the data ends, fails if collecting it does, and closes on `close()`, after which pending and later calls throw. The transport stays open, so close it once done. The write function may suspend for backpressure, but must not wait on the data being collected.

## Streams

```kotlin
router.onRequest(2u) { request ->
  Reply.Stream(request.body!!.map { it.decodeToString().uppercase().encodeToByteArray() })
}

val body = flowOf("ada", "grace").map { it.encodeToByteArray() }

rpc.requestFlow(2u, body).collect { println(it.decodeToString()) }
```

`requestFlow` streams a request body and collects the reply streamed back.

## License

Apache-2.0
