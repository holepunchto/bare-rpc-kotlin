package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

class SharedIdTest {
  private val open = "0400000001010701" // request id 1, command 7, opens a stream
  private val ack = "050000000301fd0101" // stream id 1, request open
  private val data = "080000000301fd1001026869" // stream id 1, request data "hi"
  private val end = "050000000301fd2001" // stream id 1, request end
  private val pause = "050000000301fd0401" // stream id 1, request pause
  private val resume = "050000000301fd0801" // stream id 1, request resume
  private val destroy = "050000000301fd4001" // stream id 1, request destroy
  private val hi = "0700000002010000026869" // response id 1, "hi"
  private val ok = "0700000002010000026f6b" // response id 1, "ok"
  private val request = "0700000001010700026869" // request id 1, command 7, "hi"
  private val replyOpen = "0400000002010001" // response id 1, opens a stream
  private val replyAck = "050000000301fd0102" // stream id 1, response open
  private val replyData = "080000000301fd1002026869" // stream id 1, response data "hi"
  private val replyEnd = "050000000301fd2002" // stream id 1, response end
  private val replyPause = "050000000301fd0402" // stream id 1, response pause
  private val replyResume = "050000000301fd0802" // stream id 1, response resume
  private val replyDestroy = "050000000301fd4002" // stream id 1, response destroy

  @Test
  fun `request streams of the same id on both sides stay apart`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Data(it.body!!.toList().reduce(ByteArray::plus)) },
        scope = backgroundScope,
      )
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send(open.hexToByteArray())
    remote.send((pause + ack).hexToByteArray())
    remote.send(data.hexToByteArray())
    remote.send(destroy.hexToByteArray())
    remote.send(resume.hexToByteArray())
    remote.send(end.hexToByteArray())
    assertEquals(listOf(open, ack, hi), sent)
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `reply streams of the same id on both sides stay apart`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc =
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = { Reply.Stream(flowOf("hi".encodeToByteArray())) },
        scope = backgroundScope,
      )
    val chunks = async { rpc.requestFlow(7u, "hi".encodeToByteArray()).toList() }
    yield()
    remote.send(request.hexToByteArray())
    remote.send(replyOpen.hexToByteArray())
    remote.send((replyPause + replyAck).hexToByteArray())
    remote.send(replyData.hexToByteArray())
    remote.send(replyDestroy.hexToByteArray())
    remote.send(replyResume.hexToByteArray())
    remote.send(replyEnd.hexToByteArray())
    assertEquals(listOf("hi"), chunks.await().map { it.decodeToString() })
    assertEquals(listOf(request, replyAck, replyOpen), sent)
  }
}
