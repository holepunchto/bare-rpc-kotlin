package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

@OptIn(ExperimentalCoroutinesApi::class)
class ResponseStreamTest {
  private val open = "0400000002010001" // response id 1, opens a stream
  private val ack = "050000000301fd0102" // stream id 1, response open
  private val data = "080000000301fd1002026869" // stream id 1, response data "hi"
  private val end = "050000000301fd2002" // stream id 1, response end
  private val request = "0700000001010700026869" // request id 1, command 7, "hi"
  private val closeError =
    "120000000301fd820204626f6f6d06455f424f4f4d02" // stream id 1, response close error: "boom",
  // "E_BOOM", errno 1
  private val pause = "050000000301fd0402" // stream id 1, response pause
  private val resume = "050000000301fd0802" // stream id 1, response resume
  private val destroy = "050000000301fd4002" // stream id 1, response destroy
  // stream id 1, response destroy error: "boom", "E_BOOM", errno 1
  private val destroyError = "120000000301fdc00204626f6f6d06455f424f4f4d02"

  @Test
  fun `a streamed reply opens and waits for the ack before sending data`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = backgroundScope,
      )
    remote.send(request.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `after the ack the reply goes out as data then end`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = backgroundScope,
      )
    remote.send(request.hexToByteArray())
    yield()
    remote.send(ack.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
  }

  @Test
  fun `an ack that arrives before the reply is kept`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = backgroundScope,
      )
    remote.send(ack.hexToByteArray())
    remote.send(request.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
  }

  @Test
  fun `a reply body that throws closes the stream with the error`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flow { throw RemoteException("boom", "E_BOOM", 1) }) },
        scope = backgroundScope,
      )
    remote.send(request.hexToByteArray())
    yield()
    remote.send(ack.hexToByteArray())
    yield()
    assertEquals(listOf(open, closeError), sent)
  }

  @Test
  fun `a pause holds the reply body until resume`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = backgroundScope,
      )
    remote.send(request.hexToByteArray())
    remote.send((ack + pause).hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    remote.send(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
  }

  @Test
  fun `close releases a paused reply body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = backgroundScope,
      )
    remote.send(request.hexToByteArray())
    remote.send((ack + pause).hexToByteArray())
    yield()
    rpc.close()
    assertEquals(listOf(open), sent)
  }

  @Test
  fun `a destroy stops the reply body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = backgroundScope,
      )
    remote.send(request.hexToByteArray())
    remote.send((ack + pause).hexToByteArray())
    yield()
    remote.send(destroy.hexToByteArray())
    remote.send(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
  }

  @Test
  fun `a destroy with an error stops the reply body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = backgroundScope,
      )
    remote.send(request.hexToByteArray())
    remote.send((ack + pause).hexToByteArray())
    yield()
    remote.send(destroyError.hexToByteArray())
    remote.send(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
  }

  @Test
  fun `a destroy that comes with the request stops the reply body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = backgroundScope,
      )
    remote.send((request + ack + destroy).hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
  }

  @Test
  fun `close stops a reply body that is still running`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
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
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(body) },
        scope = backgroundScope,
      )
    remote.send(request.hexToByteArray())
    remote.send(ack.hexToByteArray())
    assertEquals(listOf(open, data), sent)
    rpc.close()
    runCurrent()
    assertTrue(stopped.isCompleted)
  }

  @Test
  fun `a reply stream the peer destroys before the reply is not streamed`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val replying = CompletableDeferred<Unit>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = {
          replying.await()
          Reply.Stream(flowOf("hi".encodeToByteArray()))
        },
        scope = backgroundScope,
      )
    remote.send(Message.Request(1u, 7u, STREAM_OPEN, null))
    remote.send((ack + destroy).hexToByteArray())
    replying.complete(Unit)
    runCurrent()
    assertTrue(open in sent)
    assertTrue(data !in sent && end !in sent)
    assertEquals(0, rpc.tracked())
  }

  @Test
  fun `a streamed reply on a cancelled scope fails the request instead of opening`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val scope = CoroutineScope(backgroundScope.coroutineContext + Job())
    Rpc(
      TestTransport(remote.data, { sent += it.toHexString() }),
      onRequest = {
        scope.cancel()
        Reply.Stream(flowOf("hi".encodeToByteArray()))
      },
      scope = scope,
    )
    remote.send(request.hexToByteArray())
    val reply = Messages.decode(sent.single().hexToByteArray()).message as Message.Response
    assertEquals(1uL to 0uL, reply.id to reply.stream)
    assertNotNull(reply.error)
  }
}
