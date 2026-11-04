package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import to.holepunch.compactEncoding.encode

@OptIn(ExperimentalCoroutinesApi::class)
class SessionEndTest {
  private val malformed = "020000000100"
  private val ack = "050000000301fd0101" // stream id 1, request open

  private fun causes(e: Throwable?) = generateSequence(e) { it.cause }

  private fun event(command: ULong) =
    encode(frame, Message.Request(0u, command, 0u, ByteArray(0))).toHexString()

  @Test
  fun `close writes the frames already queued and then closes the transport`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val gate = CompletableDeferred<Unit>()
    val transport =
      TestTransport(remote.data) {
        gate.await()
        sent += it.toHexString()
      }
    val rpc = Rpc(transport, scope = backgroundScope)
    launch { rpc.event(1u, ByteArray(0)) }
    launch { rpc.event(2u, ByteArray(0)) }
    runCurrent()
    val closing = launch { rpc.close() }
    runCurrent()
    assertEquals(0, transport.closes)
    gate.complete(Unit)
    closing.join()
    assertEquals(listOf(event(1u), event(2u)), sent)
    assertEquals(1, transport.closes)
  }

  @Test
  fun `a failure drops the frames queued and closes the transport`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val gate = CompletableDeferred<Unit>()
    val transport =
      TestTransport(remote.data) {
        gate.await()
        sent += it.toHexString()
      }
    val rpc = Rpc(transport, scope = backgroundScope)
    launch { rpc.event(1u, ByteArray(0)) }
    launch { rpc.event(2u, ByteArray(0)) }
    runCurrent()
    remote.send(malformed.hexToByteArray())
    gate.complete(Unit)
    assertFailsWith<MalformedFrameException> { rpc.wait() }
    assertEquals(listOf(event(1u)), sent)
    assertEquals(1, transport.closes)
  }

  @Test
  fun `close returns once the transport is closed`() = runTest {
    val gate = CompletableDeferred<Unit>()
    val transport = Transport(flow { awaitCancellation() }, {}, gate::await)
    val rpc = Rpc(transport, scope = backgroundScope)
    val closing = launch { rpc.close() }
    runCurrent()
    assertFalse(closing.isCompleted)
    gate.complete(Unit)
    runCurrent()
    assertTrue(closing.isCompleted)
  }

  @Test
  fun `the end of the data closes the transport and wait returns`() = runTest {
    val remote = Remote(this)
    val transport = TestTransport(remote.data)
    val rpc = Rpc(transport, scope = backgroundScope)
    remote.end()
    rpc.wait()
    assertEquals(1, transport.closes)
  }

  @Test
  fun `wait returns after close`() = runTest {
    val transport = TestTransport(flow { awaitCancellation() })
    val rpc = Rpc(transport, scope = backgroundScope)
    val waiting = async { rpc.wait() }
    runCurrent()
    assertFalse(waiting.isCompleted)
    rpc.close()
    waiting.await()
    assertEquals(1, transport.closes)
  }

  @Test
  fun `wait throws the failure that closed the connection`() = runTest {
    val failure = IllegalStateException("transport down")
    val transport = TestTransport(flow { throw failure })
    val rpc = Rpc(transport, scope = backgroundScope)
    assertSame(failure, assertFailsWith<IllegalStateException> { rpc.wait() })
    assertEquals(1, transport.closes)
  }

  @Test
  fun `a partial frame at the end of the data makes wait throw`() = runTest {
    val rpc = Rpc(TestTransport(flowOf("0400000001".hexToByteArray())), scope = backgroundScope)
    assertFailsWith<MalformedFrameException> { rpc.wait() }
  }

  @Test
  fun `a failure stops a request body with the failure as its cause`() = runTest {
    val remote = Remote(this)
    val stopped = CompletableDeferred<Throwable?>()
    val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
    val body = flow<ByteArray> { awaitCancellation() }.onCompletion { stopped.complete(it) }
    launch { runCatching { rpc.request(7u, body) } }
    yield()
    remote.send(ack.hexToByteArray())
    remote.send(malformed.hexToByteArray())
    assertTrue(causes(stopped.await()).any { it is MalformedFrameException })
  }

  @Test
  fun `close stops a streamed reply with a connection closed cause`() = runTest {
    val remote = Remote(this)
    val stopped = CompletableDeferred<Throwable?>()
    val body = flow<ByteArray> { awaitCancellation() }.onCompletion { stopped.complete(it) }
    val rpc = Rpc(TestTransport(remote.data), backgroundScope, onRequest = { Reply.Stream(body) })
    remote.send(Message.Request(1u, 7u, 0u, ByteArray(0)))
    remote.send(Message.Stream(1u, STREAM_RESPONSE or STREAM_OPEN, null, null))
    rpc.close()
    assertTrue(causes(stopped.await()).any { it is ConnectionClosedException })
  }
}
