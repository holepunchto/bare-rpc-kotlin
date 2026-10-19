# bare-rpc-kotlin

https://github.com/holepunchto/librpc ABI compatible RPC for Kotlin

## Install

Published through [JitPack](https://jitpack.io/#holepunchto/bare-rpc-kotlin):

```kotlin
repositories {
  maven("https://jitpack.io")
}

dependencies {
  implementation("com.github.holepunchto.bare-rpc-kotlin:bare-rpc-kotlin:<tag>")
}
```

## Usage

[`UsageTest.kt`](src/commonTest/kotlin/to/holepunch/barerpc/usage/UsageTest.kt) connects two ends over in-memory channels and answers a request through a `CommandRouter`, then streams a request body and reads the reply streamed back with `requestFlow`. The end that reads a request body or streams a reply needs a `scope` to run in, as the server has here. A real transport takes the channels' place: give `BareRpc` a `send` that writes each frame to it, and pass every chunk it reads to `receive` from a single read loop. When `receive` throws, the connection is closed: stop the loop and close the transport. When the transport ends, or you close it, call `BareRpc.close`: pending and later calls then throw `ConnectionClosedException`. `send` may suspend for backpressure, but must not wait on this end's own read loop.

## API

The KDoc on each public declaration is the reference. `./gradlew dokkaGenerate` renders it to `build/dokka/html`.

## Build

Building needs:

- JDK 21
- Node.js, whose `npm` fetches the wire fixtures and whose `node` runs the JS bare-rpc peer
- An Android SDK with platform `android-36`, found through `ANDROID_HOME` or `sdk.dir` in `local.properties`

```sh
./gradlew check
```

## License

Apache-2.0
