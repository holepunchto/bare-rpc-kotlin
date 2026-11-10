package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield

class IncomingRequestTest {
  private val hi = "0700000001050700026869" // request id 5, command 7, "hi"
  private val first = "0700000001010700026869" // request id 1, command 7, "hi"
  private val ok = "0700000002050000026f6b" // response id 5, "ok"
  // cancel id 1, CANCELLED
  private val cancel =
    "2300000004011552657175657374207761732063616e63656c6c65640943414e43454c4c454400"
  private val boom = "110000000205010004626f6f6d06455f424f4f4d02" // error id 5: "boom", "E_BOOM", 1
  private val secret = "0d00000002050100067365637265740000" // error id 5: "secret", "", 0
  private val noDirection = "03000000030104" // stream id 1, pause, neither request nor response

  @Test
  fun `a request reaches the handler and its return value goes back`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val seen = mutableListOf<Pair<ULong, String>>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = {
          seen += it.command to it.data.decodeToString()
          Reply.Data("ok".encodeToByteArray())
        },
        scope = backgroundScope,
      )
    remote.send(hi.hexToByteArray())
    assertEquals(listOf(7uL to "hi"), seen)
    assertEquals(listOf(ok), sent)
  }

  @Test
  fun `any other exception from the handler goes back with its message`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { error("secret") },
        scope = backgroundScope,
      )
    remote.send(hi.hexToByteArray())
    assertEquals(listOf(secret), sent)
  }

  @Test
  fun `a request with no handler is rejected as an unknown command`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    remote.send(hi.hexToByteArray())
    val reply = Messages.decode(sent.single().hexToByteArray()).message as Message.Response
    assertEquals(5uL, reply.id)
    assertEquals("UNKNOWN_COMMAND" to 0L, reply.error?.let { it.code to it.errno })
  }

  @Test
  fun `a throwing handler leaves the connection open`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { error("secret") },
        scope = backgroundScope,
      )
    remote.send(hi.hexToByteArray())
    remote.send(hi.hexToByteArray())
    assertEquals(listOf(secret, secret), sent)
  }

  @Test
  fun `a remote exception from the handler goes back as that error`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { throw RemoteException("boom", "E_BOOM", 1) },
        scope = backgroundScope,
      )
    remote.send(hi.hexToByteArray())
    assertEquals(listOf(boom), sent)
  }

  @Test
  fun `a handler that times out goes back as an error`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { withTimeout(1) { awaitCancellation() } },
        scope = backgroundScope,
      )
    remote.send(hi.hexToByteArray())
    delay(2)
    assertEquals(1, sent.size)
  }

  @Test
  fun `a reply does not wait behind a blocked send`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val gate = CompletableDeferred<Unit>()
    val rpc =
      Rpc(
        TestTransport(remote.data) {
          if (sent.isEmpty()) {
            sent += "blocked"
            gate.await()
          } else sent += it.toHexString()
        },
        onRequest = { Reply.Data("ok".encodeToByteArray()) },
        scope = backgroundScope,
      )
    launch { rpc.request(7u, ByteArray(0)) }
    yield()
    remote.send(hi.hexToByteArray())
    gate.complete(Unit)
    delay(1)
    assertEquals(listOf("blocked", ok), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a failing send closes the connection`() = runTest {
    val remote = Remote(this)
    val failure = IllegalStateException("transport down")
    val rpc = Rpc(TestTransport(remote.data, { throw failure }), scope = backgroundScope)
    rpc.event(7u, ByteArray(0))
    assertSame(failure, assertFailsWith<IllegalStateException> { rpc.wait() })
    remote.send("0700000001000700026869".hexToByteArray())
    assertSame(failure, assertFailsWith<IllegalStateException> { rpc.request(0u, ByteArray(0)) })
  }

  @Test
  fun `cancelling a blocked request still sends the frames queued behind it`() = runTest {
    val remote = Remote(this)
    val gate = CompletableDeferred<Unit>()
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(
          remote.data,
          {
            val hex = it.toHexString()
            if (hex == first) gate.await()
            sent += hex
          },
        ),
        onRequest = { Reply.Data("ok".encodeToByteArray()) },
        scope = backgroundScope,
      )
    val call = launch { rpc.request(7u, "hi".encodeToByteArray()) }
    yield()
    remote.send(hi.hexToByteArray())
    call.cancel()
    gate.complete(Unit)
    yield()
    assertEquals(listOf(first, ok, cancel), sent)
  }

  @Test
  fun `a stream frame without a direction fails the connection`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(TestTransport(remote.data, { sent += it.toHexString() }), scope = backgroundScope)
    remote.send(noDirection.hexToByteArray())
    remote.send(hi.hexToByteArray())
    assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) }
    assertEquals(emptyList(), sent)
  }

  @Test
  fun `a request handler can await a request to the same peer`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    lateinit var rpc: Rpc
    rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(rpc.request(7u, it.data)) },
        scope = backgroundScope,
      )
    remote.send(hi.hexToByteArray())
    assertEquals(listOf(first), sent)
    remote.send("0700000002010000026f6b".hexToByteArray()) // response id 1, "ok"
    assertEquals(listOf(first, ok), sent)
  }

  @Test
  fun `a slow request handler holds back no later frame`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val gate = CompletableDeferred<Unit>()
    Rpc(
      TestTransport(remote.data, { sent += it.toHexString() }),
      onRequest = {
        if (it.data.decodeToString() == "hi") gate.await()
        Reply.Data("ok".encodeToByteArray())
      },
      scope = backgroundScope,
    )
    remote.send(hi.hexToByteArray())
    remote.send("0700000001060700026f6b".hexToByteArray()) // request id 6, command 7, "ok"
    assertEquals(listOf("0700000002060000026f6b"), sent) // response id 6, "ok"
    gate.complete(Unit)
    yield()
    assertEquals(listOf("0700000002060000026f6b", ok), sent)
  }
}
