package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

class IncomingRequestStreamTest {
  private val big =
    "084000000301fd1001fd0040" + "00".repeat(16384) // stream id 1, request data, 16 KiB
  private val pause = "050000000301fd0401" // stream id 1, request pause
  private val resume = "050000000301fd0801" // stream id 1, request resume
  private val open = "0400000001010701" // request id 1, command 7, opens a stream
  private val ack = "050000000301fd0101" // stream id 1, request open
  private val data = "080000000301fd1001026869" // stream id 1, request data "hi"
  private val end = "050000000301fd2001" // stream id 1, request end
  private val reply = "0700000002010000026869" // response id 1, "hi"
  private val noScope =
    "2f000000020101001c526571756573742073747265616d73206e65656420612073636f70650c4552525f4e4f5f53434f504500" // error id 1: "Request streams need a scope", "ERR_NO_SCOPE", errno 0
  private val close = "050000000301fd0201" // stream id 1, request close
  private val destroy = "050000000301fd4001" // stream id 1, request destroy
  // stream id 1, request close error: "boom", "E_BOOM", errno 1
  private val closeError = "120000000301fd820104626f6f6d06455f424f4f4d02"
  private val boom =
    "110000000201010004626f6f6d06455f424f4f4d02" // error id 1: "boom", "E_BOOM", errno 1
  // stream id 1, request end with an error: "boom", "E_BOOM", errno 1
  private val endError = "120000000301fda00104626f6f6d06455f424f4f4d02"
  private val empty = "050000000201000000" // response id 1, empty

  @Test
  fun `collecting a request stream sends the ack`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = {
          it.body!!.collect {}
          Reply.Data(ByteArray(0))
        },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    assertEquals(listOf(ack), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a request stream's data reaches the handler and its reply goes back`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(data.hexToByteArray())
    rpc.receive(end.hexToByteArray())
    yield()
    assertEquals(listOf(ack, reply), sent)
  }

  @Test
  fun `a request stream without a scope is answered with an error`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() })
    rpc.receive(open.hexToByteArray())
    assertEquals(listOf(noScope), sent)
  }

  @Test
  fun `a close without an error ends the body`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(data.hexToByteArray())
    rpc.receive(close.hexToByteArray())
    yield()
    assertEquals(listOf(ack, reply), sent)
  }

  @Test
  fun `close ends a request body with the cause`() = runTest {
    val caught = CompletableDeferred<Throwable>()
    val rpc =
      BareRpc(
        send = {},
        onRequest = {
          try {
            it.body!!.collect {}
          } catch (e: Throwable) {
            caught.complete(e)
          }
          Reply.Data(ByteArray(0))
        },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.close()
    assertIs<ConnectionClosedException>(caught.await())
  }

  @Test
  fun `a stream request has empty data`() = runTest {
    val seen = CompletableDeferred<Int>()
    val rpc =
      BareRpc(
        send = {},
        onRequest = {
          seen.complete(it.data.size)
          it.body!!.collect {}
          Reply.Data(ByteArray(0))
        },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    assertEquals(seen.await(), 0)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a body that reaches the mark pauses the peer`() = runTest {
    val release = CompletableDeferred<Unit>()
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = {
          it.body!!.collect { release.await() }
          Reply.Data(ByteArray(0))
        },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(data.hexToByteArray())
    yield()
    rpc.receive(big.hexToByteArray())
    assertEquals(listOf(ack, pause), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a paused body resumes the peer once drained`() = runTest {
    val release = CompletableDeferred<Unit>()
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = {
          it.body!!.collect { release.await() }
          Reply.Data(ByteArray(0))
        },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(data.hexToByteArray())
    yield()
    rpc.receive(big.hexToByteArray())
    release.complete(Unit)
    yield()
    assertEquals(listOf(ack, pause, resume), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `close still hands over a paused body's buffered chunks`() = runTest {
    val release = CompletableDeferred<Unit>()
    var taken = 0
    val rpc =
      BareRpc(
        send = {},
        onRequest = {
          try {
            it.body!!.collect {
              taken++
              release.await()
            }
          } catch (e: ConnectionClosedException) {}
          Reply.Data(ByteArray(0))
        },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(data.hexToByteArray())
    yield()
    rpc.receive(big.hexToByteArray())
    rpc.close()
    release.complete(Unit)
    yield()
    assertEquals(2, taken)
  }

  @Test
  fun `a request stream past the cap fails the connection`() = runTest {
    val rpc =
      BareRpc(
        send = {},
        onRequest = { awaitCancellation() },
        maxBufferedBytes = 1u,
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    assertFailsWith<StreamOverflowException> { rpc.receive(data.hexToByteArray()) }
    assertFailsWith<StreamOverflowException> { rpc.receive(end.hexToByteArray()) }
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a request stream up to the cap is kept`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        maxBufferedBytes = 2u,
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(data.hexToByteArray())
    rpc.receive(end.hexToByteArray())
    yield()
    assertEquals(listOf(ack, reply), sent)
  }

  @Test
  fun `a close with an error fails the body`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(closeError.hexToByteArray())
    yield()
    assertEquals(listOf(ack, boom), sent)
  }

  @Test
  fun `an end with an error ends the body`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(endError.hexToByteArray())
    yield()
    assertEquals(listOf(ack, empty), sent)
  }

  @Test
  fun `a handler that stops reading early sends a destroy`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Data(it.body!!.first()) },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(data.hexToByteArray())
    yield()
    assertEquals(listOf(ack, destroy, reply), sent)
  }

  @Test
  fun `a handler that ignores the body sends a destroy`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Data(ByteArray(0)) },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    assertEquals(listOf(empty, destroy), sent)
  }

  @Test
  fun `a close after the end is ignored`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    rpc.receive(data.hexToByteArray())
    rpc.receive(end.hexToByteArray())
    rpc.receive(close.hexToByteArray())
    yield()
    assertEquals(listOf(ack, reply), sent)
  }
}
