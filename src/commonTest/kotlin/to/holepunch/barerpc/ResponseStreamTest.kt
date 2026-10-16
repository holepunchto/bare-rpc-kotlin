package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

class ResponseStreamTest {
  private val open = "0400000002010001" // response id 1, opens a stream
  private val ack = "050000000301fd0102" // stream id 1, response open
  private val data = "080000000301fd1002026869" // stream id 1, response data "hi"
  private val end = "050000000301fd2002" // stream id 1, response end
  private val noScope =
    "2f000000020101001c526571756573742073747265616d73206e65656420612073636f70650c4552525f4e4f5f53434f504500" // error id 1: "Request streams need a scope", "ERR_NO_SCOPE", errno 0
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
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = this,
      )
    rpc.receive(request.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `after the ack the reply goes out as data then end`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = this,
      )
    rpc.receive(request.hexToByteArray())
    yield()
    rpc.receive(ack.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
  }

  @Test
  fun `an ack that arrives before the reply is kept`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = this,
      )
    rpc.receive(ack.hexToByteArray())
    rpc.receive(request.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
  }

  @Test
  fun `a streamed reply without a scope is answered with an error`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
      )
    rpc.receive(request.hexToByteArray())
    assertEquals(listOf(noScope), sent)
  }

  @Test
  fun `a reply body that throws closes the stream with the error`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Stream(flow { throw RemoteException("boom", "E_BOOM", 1) }) },
        scope = this,
      )
    rpc.receive(request.hexToByteArray())
    yield()
    rpc.receive(ack.hexToByteArray())
    yield()
    assertEquals(listOf(open, closeError), sent)
  }

  @Test
  fun `a pause holds the reply body until resume`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = this,
      )
    rpc.receive(request.hexToByteArray())
    rpc.receive(ack.hexToByteArray())
    rpc.receive(pause.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    rpc.receive(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
  }

  @Test
  fun `close releases a paused reply body`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = this,
      )
    rpc.receive(request.hexToByteArray())
    rpc.receive(ack.hexToByteArray())
    rpc.receive(pause.hexToByteArray())
    yield()
    rpc.close()
    assertEquals(listOf(open), sent)
  }

  @Test
  fun `a destroy stops the reply body`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = this,
      )
    rpc.receive(request.hexToByteArray())
    rpc.receive(ack.hexToByteArray())
    rpc.receive(pause.hexToByteArray())
    yield()
    rpc.receive(destroy.hexToByteArray())
    rpc.receive(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
  }

  @Test
  fun `a destroy with an error stops the reply body`() = runTest {
    val sent = mutableListOf<String>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = this,
      )
    rpc.receive(request.hexToByteArray())
    rpc.receive(ack.hexToByteArray())
    rpc.receive(pause.hexToByteArray())
    yield()
    rpc.receive(destroyError.hexToByteArray())
    rpc.receive(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
  }
}
