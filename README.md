# bare-rpc-kotlin

https://github.com/holepunchto/librpc ABI compatible RPC for Kotlin

## Install

Published through [JitPack](https://jitpack.io/#holepunchto/bare-rpc-kotlin):

```kotlin
repositories {
  maven("https://jitpack.io")
}

dependencies {
  implementation("com.github.holepunchto:bare-rpc-kotlin:<tag>")
}
```

## Build

Building needs:

- JDK 21
- Node.js, whose `npm` fetches the wire fixtures
- An Android SDK with platform `android-36`, found through `ANDROID_HOME` or `sdk.dir` in `local.properties`

```sh
./gradlew check
```

## License

Apache-2.0
