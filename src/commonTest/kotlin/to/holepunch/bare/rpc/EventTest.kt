package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.test.runTest

class EventTest {
  private val hi = "07000000" + "01000700026869" // event, command 7, data "hi"

  @Test
  fun `an event goes out as one frame with id 0`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
      .event(7u, "hi".encodeToByteArray())
    assertEquals(listOf(hi), sent)
  }

  @Test
  fun `a received event reaches the handler`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<Pair<ULong, String>>()
    Rpc(
      TestTransport(remote.data),
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
      Rpc(TestTransport(remote.data), onEvent = { events += it.command }, scope = backgroundScope)
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
      Rpc(TestTransport(remote.data), onEvent = { events += it.command }, scope = backgroundScope)
    remote.send("020000000100".hexToByteArray())
    val failure = assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) }
    remote.send(hi.hexToByteArray())
    assertSame(failure, assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) })
    assertEquals(emptyList(), events)
  }

  @Test
  fun `a frame over the cap fails receive`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(TestTransport(remote.data), maxFrameSize = 8, scope = backgroundScope)
    remote.send("05000000".hexToByteArray())
    assertFailsWith<FrameTooLargeException> { rpc.request(0u, ByteArray(0)) }
  }

  @Test
  fun `a throwing event handler fails this receive and every later one`() = runTest {
    val remote = Remote(this)
    val rpc =
      Rpc(
        TestTransport(remote.data),
        onEvent = { error("handler failed") },
        scope = backgroundScope,
      )
    val failure = run {
      remote.send(hi.hexToByteArray())
      assertFailsWith<IllegalStateException> { rpc.request(0u, ByteArray(0)) }
    }
    remote.send(hi.hexToByteArray())
    assertSame(failure, assertFailsWith<IllegalStateException> { rpc.request(0u, ByteArray(0)) })
  }

  @Test
  fun `a frame over the cap after a whole one fails the same receive`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<ULong>()
    val rpc =
      Rpc(TestTransport(remote.data), onEvent = { events += it.command }, scope = backgroundScope)
    remote.send((hi + "ffffffff").hexToByteArray())
    assertFailsWith<FrameTooLargeException> { rpc.request(0u, ByteArray(0)) }
    assertEquals(listOf(7uL), events)
  }

  @Test
  fun `an event after a failure is dropped`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it }), scope = backgroundScope)
    remote.send("020000000100".hexToByteArray())
    rpc.event(7u, ByteArray(0))
    assertEquals(emptyList(), sent)
  }

  @Test
  fun `an event that opens a stream is malformed`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<ULong>()
    val rpc =
      Rpc(TestTransport(remote.data), onEvent = { events += it.command }, scope = backgroundScope)
    remote.send("0400000001000701".hexToByteArray())
    assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) }
    assertEquals(emptyList(), events)
  }

  @Test
  fun `sizes that are not positive are rejected`() = runTest {
    assertFailsWith<IllegalArgumentException> {
      Rpc(TestTransport(emptyFlow()), maxFrameSize = 0, scope = backgroundScope)
    }
    assertFailsWith<IllegalArgumentException> {
      Rpc(TestTransport(emptyFlow()), maxBufferedBytes = 0, scope = backgroundScope)
    }
  }

  @Test
  fun `an event handler can await a request to the same peer`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val replies = mutableListOf<String>()
    lateinit var rpc: Rpc
    rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onEvent = { replies += rpc.request(7u, it.data).decodeToString() },
        scope = backgroundScope,
      )
    remote.send(hi.hexToByteArray())
    assertEquals(listOf("0700000001010700026869"), sent) // request id 1, command 7, "hi"
    remote.send("0700000002010000026f6b".hexToByteArray()) // response id 1, "ok"
    assertEquals(listOf("ok"), replies)
  }
}
