package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.test.runTest

class CommandRouterTest {
  private val seven = "0700000001050700026869" // request id 5, command 7, "hi"
  private val eight = "0700000001050800026869" // request id 5, command 8, "hi"
  private val ok = "0700000002050000026f6b" // response id 5, "ok"
  private val unknownSeven =
    "2b0000000205010011556e6b6e6f776e20636f6d6d616e642037134552525f554e4b4e4f574e5f434f4d4d414e4400"
  private val eventSeven = "0700000001000700026869" // event, command 7, "hi"
  private val eventEight = "0700000001000800026869" // event, command 8, "hi"

  @Test
  fun `a request goes to the handler for its command`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val router = CommandRouter()
    router.onRequest(7u) { error("wrong command") }
    router.onRequest(8u) { Reply.Data("ok".encodeToByteArray()) }
    Rpc(
      remote.data,
      write = { sent += it.toHexString() },
      onRequest = router::dispatch,
      scope = backgroundScope,
    )
    remote.send(eight.hexToByteArray())
    assertEquals(listOf(ok), sent)
  }

  @Test
  fun `a request for no registered command is rejected as unknown`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val router = CommandRouter()
    router.onRequest(8u) { Reply.Data("ok".encodeToByteArray()) }
    Rpc(
      remote.data,
      write = { sent += it.toHexString() },
      onRequest = router::dispatch,
      scope = backgroundScope,
    )
    remote.send(seven.hexToByteArray())
    assertEquals(listOf(unknownSeven), sent)
  }

  @Test
  fun `an event goes to the handler for its command`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<String>()
    val router = CommandRouter()
    router.onEvent(7u) { events += "seven" }
    router.onEvent(8u) { events += "eight" }
    Rpc(remote.data, write = {}, onEvent = router::dispatch, scope = backgroundScope)
    remote.send(eventEight.hexToByteArray())
    assertEquals(listOf("eight"), events)
  }

  @Test
  fun `an event for no registered command is dropped`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<String>()
    val router = CommandRouter()
    router.onEvent(8u) { events += "eight" }
    val rpc = Rpc(remote.data, write = {}, onEvent = router::dispatch, scope = backgroundScope)
    remote.send(eventSeven.hexToByteArray())
    remote.send(eventEight.hexToByteArray())
    assertEquals(listOf("eight"), events)
  }
}
