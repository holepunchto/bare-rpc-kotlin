package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield

class CloseTest {
  @Test
  fun `close resumes a pending request with a connection closed exception`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
      val reply = async { rpc.request(7u, ByteArray(0)) }
      yield()
      rpc.close()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `every request after close throws and every event is dropped`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it }), scope = backgroundScope)
    rpc.close()
    assertFailsWith<ConnectionClosedException> { rpc.request(7u, ByteArray(0)) }
    rpc.event(7u, ByteArray(0))
    remote.send("0700000001000700026869".hexToByteArray())
    assertFailsWith<ConnectionClosedException> { rpc.request(0u, ByteArray(0)) }
    assertEquals(emptyList(), sent)
  }

  @Test
  fun `close after a failure keeps the failure`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
    remote.send("020000000100".hexToByteArray())
    assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) }
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
        TestTransport(remote.data),
        onEvent = {
          events += it.command
          rpc.close()
        },
        scope = backgroundScope,
      )
    val two = "0700000001000700026869" + "0700000001000800026869"
    remote.send(two.hexToByteArray())
    assertFailsWith<ConnectionClosedException> { rpc.request(0u, ByteArray(0)) }
    assertEquals(listOf(7uL), events)
  }

  @Test
  fun `the end of the data closes the connection`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(TestTransport(remote.data), scope = backgroundScope)
      val reply = async { rpc.request(7u, ByteArray(0)) }
      yield()
      remote.end()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `a failure to collect the data closes the connection with it`() = runTest {
    val failure = IllegalStateException("transport down")
    val rpc = Rpc(TestTransport(flow { throw failure }), scope = backgroundScope)
    yield()
    assertSame(failure, assertFailsWith<IllegalStateException> { rpc.request(7u, ByteArray(0)) })
  }

  @Test
  fun `the end of the data stops reply bodies and reply flows`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val stopped = CompletableDeferred<Unit>()
      val body = flow {
        emit("hi".encodeToByteArray())
        try {
          awaitCancellation()
        } finally {
          stopped.complete(Unit)
        }
      }
      val rpc =
        Rpc(TestTransport(remote.data), onRequest = { Reply.Stream(body) }, scope = backgroundScope)
      val chunks = async { rpc.requestFlow(7u, ByteArray(0)).toList() }
      yield()
      remote.send(Message.Request(2u, 7u, 0u, ByteArray(0)))
      remote.send(Message.Stream(2u, STREAM_RESPONSE or STREAM_OPEN, null, null))
      remote.end()
      assertFailsWith<ConnectionClosedException> { chunks.await() }
      assertTrue(stopped.isCompleted)
    }
  }

  @Test
  fun `close inside the scope returns without the data ending and releases it`() = runTest {
    val chunks = Channel<ByteArray>(Channel.UNLIMITED)
    withTimeout(1.seconds) {
      coroutineScope {
        val rpc = Rpc(TestTransport(chunks.consumeAsFlow()), scope = this)
        yield()
        rpc.close()
      }
    }
    assertTrue(chunks.trySend(ByteArray(0)).isClosed)
  }

  @Test
  fun `close from a handler and a second close both return`() = runTest {
    val remote = Remote(this)
    lateinit var rpc: Rpc
    rpc =
      Rpc(
        TestTransport(remote.data),
        onRequest = {
          rpc.close()
          Reply.Data(ByteArray(0))
        },
        scope = backgroundScope,
      )
    remote.send(Message.Request(1u, 7u, 0u, ByteArray(0)))
    withTimeout(1.seconds) { rpc.close() }
    assertFailsWith<ConnectionClosedException> { rpc.request(0u, ByteArray(0)) }
  }
}
