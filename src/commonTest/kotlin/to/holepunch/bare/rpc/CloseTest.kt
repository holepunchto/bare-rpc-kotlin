package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

class CloseTest {
  @Test
  fun `close resumes a pending request with a connection closed exception`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(remote.data, write = {}, scope = backgroundScope)
      val reply = async { rpc.request(7u, ByteArray(0)) }
      yield()
      rpc.close()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `every call after close throws`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(remote.data, write = {}, scope = backgroundScope)
    rpc.close()
    assertFailsWith<ConnectionClosedException> { rpc.request(7u, ByteArray(0)) }
    assertFailsWith<ConnectionClosedException> { rpc.event(7u, ByteArray(0)) }
    remote.send("0700000001000700026869".hexToByteArray())
    assertFailsWith<ConnectionClosedException> { rpc.event(0u, ByteArray(0)) }
  }

  @Test
  fun `close after a failure keeps the failure`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(remote.data, write = {}, scope = backgroundScope)
    remote.send("020000000100".hexToByteArray())
    assertFailsWith<MalformedFrameException> { rpc.event(0u, ByteArray(0)) }
    rpc.close()
    assertFailsWith<MalformedFrameException> { rpc.request(7u, ByteArray(0)) }
  }

  @Test
  fun `close stops the rest of the chunk`() = runTest {
    val remote = Remote(this)
    val events = mutableListOf<ULong>()
    lateinit var rpc: Rpc
    rpc =
      Rpc(
        remote.data,
        write = {},
        onEvent = {
          events += it.command
          rpc.close()
        },
        scope = backgroundScope,
      )
    val two = "0700000001000700026869" + "0700000001000800026869"
    remote.send(two.hexToByteArray())
    assertFailsWith<ConnectionClosedException> { rpc.event(0u, ByteArray(0)) }
    assertEquals(listOf(7uL), events)
  }

  @Test
  fun `the end of the data closes the connection`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(remote.data, write = {}, scope = backgroundScope)
      val reply = async { rpc.request(7u, ByteArray(0)) }
      yield()
      remote.end()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `a failure to collect the data closes the connection with it`() = runTest {
    val failure = IllegalStateException("transport down")
    val rpc = Rpc(flow { throw failure }, write = {}, scope = backgroundScope)
    yield()
    assertSame(failure, assertFailsWith<IllegalStateException> { rpc.request(7u, ByteArray(0)) })
  }
}
