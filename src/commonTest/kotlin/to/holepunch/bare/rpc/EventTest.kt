package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlinx.coroutines.test.runTest

class EventTest {
  private val hi = "07000000" + "01000700026869" // event, command 7, data "hi"

  @Test
  fun `an event goes out as one frame with id 0`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
      .event(7u, "hi".encodeToByteArray())
    assertEquals(listOf(hi), sent)
  }

  @Test
  fun `a received event reaches the handler`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<Pair<ULong, String>>()
    Rpc(
      remote.data,
      write = {},
      onEvent = { events += it.command to it.data.decodeToString() },
      scope = backgroundScope,
    )
    remote.send(hi.hexToByteArray())
    assertEquals(listOf(7uL to "hi"), events)
  }

  @Test
  fun `an event split across receives arrives once it is whole`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<ULong>()
    val rpc =
      Rpc(remote.data, write = {}, onEvent = { events += it.command }, scope = backgroundScope)
    val bytes = hi.hexToByteArray()
    remote.send(bytes.copyOfRange(0, 6))
    assertEquals(emptyList(), events)
    remote.send(bytes.copyOfRange(6, bytes.size))
    assertEquals(listOf(7uL), events)
  }

  @Test
  fun `a malformed frame fails this receive and every later one`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<ULong>()
    val rpc =
      Rpc(remote.data, write = {}, onEvent = { events += it.command }, scope = backgroundScope)
    remote.send("020000000100".hexToByteArray())
    val failure = assertFailsWith<MalformedFrameException> { rpc.event(0u, ByteArray(0)) }
    remote.send(hi.hexToByteArray())
    assertSame(failure, assertFailsWith<MalformedFrameException> { rpc.event(0u, ByteArray(0)) })
    assertEquals(emptyList(), events)
  }

  @Test
  fun `a frame over the cap fails receive`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(remote.data, write = {}, maxFrameSize = 8u, scope = backgroundScope)
    remote.send("05000000".hexToByteArray())
    assertFailsWith<MalformedFrameException> { rpc.event(0u, ByteArray(0)) }
  }

  @Test
  fun `a throwing event handler fails this receive and every later one`() = runTest {
    val remote = Remote(this)
    val rpc =
      Rpc(
        remote.data,
        write = {},
        onEvent = { error("handler failed") },
        scope = backgroundScope,
      )
    val failure = run {
      remote.send(hi.hexToByteArray())
      assertFailsWith<IllegalStateException> { rpc.event(0u, ByteArray(0)) }
    }
    remote.send(hi.hexToByteArray())
    assertSame(failure, assertFailsWith<IllegalStateException> { rpc.event(0u, ByteArray(0)) })
  }

  @Test
  fun `a frame over the cap after a whole one fails the same receive`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<ULong>()
    val rpc =
      Rpc(remote.data, write = {}, onEvent = { events += it.command }, scope = backgroundScope)
    remote.send((hi + "ffffffff").hexToByteArray())
    assertFailsWith<MalformedFrameException> { rpc.event(0u, ByteArray(0)) }
    assertEquals(listOf(7uL), events)
  }

  @Test
  fun `an event after a failure throws the same exception and sends nothing`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val rpc = Rpc(remote.data, write = { sent += it }, scope = backgroundScope)
    remote.send("020000000100".hexToByteArray())
    val failure = assertFailsWith<MalformedFrameException> { rpc.event(0u, ByteArray(0)) }
    assertSame(failure, assertFailsWith<MalformedFrameException> { rpc.event(7u, ByteArray(0)) })
    assertEquals(emptyList(), sent)
  }

  @Test
  fun `an event that opens a stream is malformed`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<ULong>()
    val rpc =
      Rpc(remote.data, write = {}, onEvent = { events += it.command }, scope = backgroundScope)
    remote.send("0400000001000701".hexToByteArray())
    assertFailsWith<MalformedFrameException> { rpc.event(0u, ByteArray(0)) }
    assertEquals(emptyList(), events)
  }
}
