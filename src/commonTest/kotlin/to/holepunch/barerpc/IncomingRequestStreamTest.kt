package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

class IncomingRequestStreamTest {
  private val open = "0400000001010701" // request id 1, command 7, opens a stream
  private val ack = "050000000301fd0101" // stream id 1, request open
  private val data = "080000000301fd1001026869" // stream id 1, request data "hi"
  private val end = "050000000301fd2001" // stream id 1, request end
  private val reply = "0700000002010000026869" // response id 1, "hi"
  private val noScope =
    "2f000000020101001c526571756573742073747265616d73206e65656420612073636f70650c4552525f4e4f5f53434f504500" // error id 1: "Request streams need a scope", "ERR_NO_SCOPE", errno 0
  private val close = "050000000301fd0201" // stream id 1, request close

  @Test
  fun `collecting a request stream sends the ack`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = {
          it.body!!.collect {}
          ByteArray(0)
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
        onRequest = { it.body!!.fold(ByteArray(0)) { a, b -> a + b } },
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
        onRequest = { it.body!!.fold(ByteArray(0)) { a, b -> a + b } },
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
          ByteArray(0)
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
          ByteArray(0)
        },
        scope = this,
      )
    rpc.receive(open.hexToByteArray())
    yield()
    assertEquals(seen.await(), 0)
    coroutineContext.cancelChildren()
  }
}
