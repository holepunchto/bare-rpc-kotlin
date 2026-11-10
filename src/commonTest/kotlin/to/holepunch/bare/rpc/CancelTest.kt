package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import to.holepunch.compactEncoding.encode

@OptIn(ExperimentalCoroutinesApi::class)
class CancelTest {
  private val gaveUp = RemoteError("Gave up", "GAVE_UP", 3)

  private fun request(id: ULong, stream: ULong = 0u) =
    Message.Request(id, 7u, stream, if (stream == 0uL) ByteArray(0) else null)

  private fun sentMessages(sent: List<ByteArray>) = sent.map { Messages.decode(it).message }

  @Test
  fun `a call that times out cancels its request with TIMEOUT`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it }), scope = backgroundScope)
    assertFailsWith<TimeoutCancellationException> {
      withTimeout(100) { rpc.request(7u, ByteArray(0)) }
    }
    val cancel = assertIs<Message.Cancel>(sentMessages(sent).last())
    assertEquals(1uL, cancel.id)
    assertEquals("TIMEOUT", cancel.error.code)
    assertEquals(0, rpc.tracked())
  }

  @Test
  fun `a reply flow that times out cancels its request with TIMEOUT`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it }), scope = backgroundScope)
    assertFailsWith<TimeoutCancellationException> {
      withTimeout(100) { rpc.requestFlow(7u, ByteArray(0)).collect() }
    }
    val messages = sentMessages(sent)
    assertEquals(3, messages.size)
    assertEquals("TIMEOUT", assertIs<Message.Cancel>(messages.last()).error.code)
    assertEquals(0, rpc.tracked())
  }

  @Test
  fun `a request body that times out on its own does not cancel the request`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it }), scope = backgroundScope)
    val body = flow<ByteArray> { withTimeout(10) { awaitCancellation() } }
    val reply = async { runCatching { rpc.request(7u, body) }.exceptionOrNull() }
    val flowReply = async { runCatching { rpc.requestFlow(7u, body).collect() }.exceptionOrNull() }
    runCurrent()
    remote.send(Message.Stream(1u, STREAM_REQUEST or STREAM_OPEN, null, null))
    remote.send(Message.Stream(2u, STREAM_REQUEST or STREAM_OPEN, null, null))
    assertIs<TimeoutCancellationException>(reply.await())
    assertIs<TimeoutCancellationException>(flowReply.await())
    assertEquals(emptyList(), sentMessages(sent).filterIsInstance<Message.Cancel>())
  }

  @Test
  fun `a cancel of the peer cancels the handler with its error`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val cancelled = CompletableDeferred<RequestCancelledException>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it }),
        onRequest = {
          try {
            awaitCancellation()
          } catch (e: RequestCancelledException) {
            cancelled.complete(e)
            throw e
          }
        },
        scope = backgroundScope,
      )
    remote.send(request(5u))
    remote.send(Message.Cancel(5u, gaveUp))
    val e = cancelled.await()
    assertEquals("GAVE_UP", e.code)
    assertEquals(3, e.errno)
    assertEquals(emptyList(), sent)
    assertEquals(0, rpc.tracked())
  }

  @Test
  fun `a cancel read with its streamed request stops the handler`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    var running = false
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it }),
        onRequest = {
          running = true
          try {
            awaitCancellation()
          } finally {
            running = false
          }
        },
        scope = backgroundScope,
      )
    remote.send(encode(frame, request(5u, STREAM_OPEN)) + encode(frame, Message.Cancel(5u, gaveUp)))
    assertFalse(running)
    assertEquals(emptyList(), sent)
    assertEquals(0, rpc.tracked())
  }

  @Test
  fun `the answer of a handler that ignores the cancel is dropped`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val gate = CompletableDeferred<Unit>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it }),
        onRequest = {
          withContext(NonCancellable) { gate.await() }
          if (it.command == 7uL) Reply.Data(ByteArray(0)) else Reply.Stream(flow {})
        },
        scope = backgroundScope,
      )
    remote.send(request(5u))
    remote.send(Message.Request(6u, 8u, 0u, ByteArray(0)))
    remote.send(Message.Cancel(5u, gaveUp))
    remote.send(Message.Cancel(6u, gaveUp))
    gate.complete(Unit)
    runCurrent()
    assertEquals(emptyList(), sent)
    assertEquals(0, rpc.tracked())
  }

  @Test
  fun `a cancel stops the bodies of the request and its reply without stream frames`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val body = CompletableDeferred<Throwable>()
    val replyBody = CompletableDeferred<Throwable>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it }),
        onRequest = { incoming ->
          launch {
            try {
              incoming.body!!.collect()
            } catch (e: Throwable) {
              body.complete(e)
            }
          }
          Reply.Stream(
            flow {
              emit("hi".encodeToByteArray())
              try {
                awaitCancellation()
              } catch (e: Throwable) {
                replyBody.complete(e)
                throw e
              }
            }
          )
        },
        scope = backgroundScope,
      )
    remote.send(request(5u, STREAM_OPEN))
    remote.send(Message.Stream(5u, STREAM_RESPONSE or STREAM_OPEN, null, null))
    val before = sent.size
    remote.send(Message.Cancel(5u, gaveUp))
    assertEquals("GAVE_UP", assertIs<RemoteException>(body.await()).code)
    assertEquals("GAVE_UP", assertIs<RequestCancelledException>(replyBody.getCompleted()).code)
    assertEquals(before, sent.size)
    assertEquals(0, rpc.tracked())
  }

  @Test
  fun `a cancel for a request that is not in progress is ignored`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<ByteArray>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it }),
        onRequest = { Reply.Data(ByteArray(0)) },
        scope = backgroundScope,
      )
    remote.send(request(5u))
    remote.send(Message.Cancel(5u, gaveUp))
    remote.send(Message.Cancel(9u, gaveUp))
    assertEquals(listOf<Message>(Message.Response(5u, 0u, ByteArray(0), null)), sentMessages(sent))
    rpc.assertOpen()
  }
}
