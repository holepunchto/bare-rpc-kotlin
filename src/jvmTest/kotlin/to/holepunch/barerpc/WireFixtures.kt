package to.holepunch.barerpc

import java.io.File

internal val fixturesDir = File(System.getProperty("hrpc-test.fixtures"))

internal val unsupported =
  setOf("boundary", "dispatch", "dispatch-sparse", "envelope", "error", "negative", "sequence")
