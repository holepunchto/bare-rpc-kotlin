package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import to.holepunch.compactEncoding.encode

@OptIn(ExperimentalCoroutinesApi::class)
class PeerValidationTest {
  private val undecodable =
    listOf(
      "020000000100", // a request with no command
      "0300000001fd01", // a request whose id is a varint cut short
      "020000006300", // message type 99
      "0700000001010700056869", // a request whose data runs past the frame
      "0700000002010100096869", // a response whose error runs past the frame
    )

  private val violations =
    listOf(
      "050000000200000000", // a response with id 0
      "050000000300fd0101", // a stream frame with id 0
      "050000000301fd0103", // a stream frame with both directions
      "03000000030101", // a stream frame with neither direction
      "050000000301fd0001", // a stream frame with no event
      "080000000301fd8001000000", // a stream frame with only the error flag
      "0400000001010702", // a request with stream flags other than open
      "0400000002010002", // a response with stream flags other than open
      "0700000002010101000000", // an error response that opens a stream
      "050000000301fd0301", // a stream frame with two events
      "050000000301fd0105", // a stream frame with an unknown flag
      "080000000301fd8101000000", // a stream open with an error
      "080000000301fd8401000000", // a stream pause with an error
      "080000000301fd8801000000", // a stream resume with an error
      "080000000301fd9001000000", // a stream data with an error
      "080000000301fda001000000", // a stream end with an error
      "0700000002010200026869", // a response whose error flag is 2
      "070000000201ff00026869", // a response whose error flag is 0xff
      "050000000400000000", // a cancel with id 0
    )

  private val validEvents =
    listOf(
      STREAM_OPEN,
      STREAM_CLOSE,
      STREAM_CLOSE or STREAM_ERROR,
      STREAM_PAUSE,
      STREAM_RESUME,
      STREAM_DATA,
      STREAM_END,
      STREAM_DESTROY,
      STREAM_DESTROY or STREAM_ERROR,
    )

  private fun TestScope.connect(
    sent: MutableList<String> = mutableListOf(),
    onRequest: suspend (IncomingRequest) -> Reply = { Reply.Data(it.data) },
  ): Pair<Remote, Rpc> {
    val remote = Remote(this)
    return remote to
      Rpc(
        TestTransport(remote.data, { sent += it.toHexString() }),
        onRequest = onRequest,
        scope = backgroundScope,
      )
  }

  private fun hex(message: Message) = encode(frame, message).toHexString()

  private fun response(id: ULong, data: String) =
    hex(Message.Response(id, 0u, data.encodeToByteArray(), null))

  @Test
  fun `an undecodable frame fails the connection`() = runTest {
    for (bytes in undecodable) {
      val (remote, rpc) = connect()
      remote.send(bytes.hexToByteArray())
      assertFailsWith<RpcException>(bytes) { rpc.request(0u, ByteArray(0)) }
    }
  }

  @Test
  fun `a frame a valid peer never sends fails the connection`() = runTest {
    for (bytes in violations) {
      val (remote, rpc) = connect()
      remote.send(bytes.hexToByteArray())
      assertFailsWith<MalformedFrameException>(bytes) { rpc.request(0u, ByteArray(0)) }
    }
  }

  @Test
  fun `every stream flag a valid peer sends is accepted`() = runTest {
    val (remote, rpc) = connect()
    for (direction in listOf(STREAM_REQUEST, STREAM_RESPONSE)) {
      for (event in validEvents) {
        val error = if (event and STREAM_ERROR != 0uL) RemoteError("boom", "E", 1) else null
        remote.send(Message.Stream(9u, direction or event, byteArrayOf(1), error))
      }
    }
    rpc.assertOpen()
  }

  @Test
  fun `a partial frame at the end of the data fails the connection`() = runTest {
    val (remote, rpc) = connect()
    remote.send("0400000001".hexToByteArray())
    remote.end()
    assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) }
  }

  @Test
  fun `a request with the id of one in progress fails the connection`() = runTest {
    val (remote, rpc) =
      connect {
        it.body?.toList()
        awaitCancellation()
      }
    val open = Message.Request(1u, 7u, STREAM_OPEN, null)
    remote.send(open)
    rpc.assertOpen()
    remote.send(open)
    assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) }
  }

  @Test
  fun `stale and unknown references are ignored and keep no state`() = runTest {
    val sent = mutableListOf<String>()
    val (remote, rpc) = connect(sent)
    remote.send(Message.Request(1u, 7u, 0u, "hi".encodeToByteArray()))
    for (id in listOf(1uL, 0x7fffuL)) {
      for (mask in listOf(STREAM_REQUEST, STREAM_RESPONSE)) {
        for (event in
          listOf(STREAM_PAUSE, STREAM_RESUME, STREAM_END, STREAM_CLOSE, STREAM_DESTROY)) {
          remote.send(Message.Stream(id, mask or event, null, null))
        }
        remote.send(Message.Stream(id, mask or STREAM_DATA, byteArrayOf(1), null))
      }
      remote.send(Message.Stream(id, STREAM_REQUEST or STREAM_OPEN, null, null))
      remote.send(Message.Response(id, 0u, byteArrayOf(1), null))
    }
    remote.send(Message.Stream(1u, STREAM_RESPONSE or STREAM_OPEN, null, null))
    assertEquals(0, rpc.tracked())
    for (id in 2uL..1025uL) remote.send(
      Message.Stream(id, STREAM_RESPONSE or STREAM_OPEN, null, null)
    )
    assertEquals(1024, rpc.tracked())
    remote.send(Message.Request(2u, 7u, 0u, "ok".encodeToByteArray()))
    assertEquals(1023, rpc.tracked())
    assertTrue(response(2u, "ok") in sent)
    remote.send(Message.Stream(4000u, STREAM_RESPONSE or STREAM_OPEN, null, null))
    rpc.assertOpen()
    remote.send(Message.Stream(4001u, STREAM_RESPONSE or STREAM_OPEN, null, null))
    assertFailsWith<MalformedFrameException> { rpc.request(0u, ByteArray(0)) }
  }

  @Test
  fun `early reply destroys count toward the cap on early acks`() = runTest {
    val (remote, rpc) = connect()
    for (id in 1uL..1024uL) {
      remote.send(Message.Stream(id, STREAM_RESPONSE or STREAM_OPEN, null, null))
      remote.send(Message.Stream(id, STREAM_RESPONSE or STREAM_DESTROY, null, null))
    }
    rpc.assertOpen()
    remote.send(Message.Stream(1025u, STREAM_RESPONSE or STREAM_OPEN, null, null))
    assertFailsWith<MalformedFrameException> { rpc.assertOpen() }
  }

  @Test
  fun `an ack for a request in progress is kept until its reply streams`() = runTest {
    val sent = mutableListOf<String>()
    val handling = CompletableDeferred<Unit>()
    val (remote, _) =
      connect(sent) {
        handling.await()
        Reply.Stream(flowOf("hi".encodeToByteArray()))
      }
    remote.send(Message.Request(1u, 7u, STREAM_OPEN, null))
    remote.send(Message.Stream(1u, STREAM_RESPONSE or STREAM_OPEN, null, null))
    handling.complete(Unit)
    runCurrent()
    assertTrue(hex(Message.Stream(1u, STREAM_RESPONSE or STREAM_END, null, null)) in sent)
  }

  @Test
  fun `frames for a stream that has ended are ignored`() = runTest {
    val sent = mutableListOf<String>()
    val (remote, rpc) =
      connect(sent) { request ->
        val body = request.body
        if (body == null) Reply.Stream(flowOf(request.data))
        else Reply.Data(body.toList().fold(ByteArray(0), ByteArray::plus))
      }
    remote.send(Message.Request(1u, 7u, STREAM_OPEN, null))
    remote.send(Message.Stream(1u, STREAM_REQUEST or STREAM_DATA, "hi".encodeToByteArray(), null))
    remote.send(Message.Stream(1u, STREAM_REQUEST or STREAM_END, null, null))
    remote.send(Message.Request(2u, 7u, 0u, "hi".encodeToByteArray()))
    remote.send(Message.Stream(2u, STREAM_RESPONSE or STREAM_OPEN, null, null))
    for (id in listOf(1uL, 2uL)) {
      for (mask in listOf(STREAM_REQUEST, STREAM_RESPONSE)) {
        for (event in listOf(STREAM_OPEN, STREAM_PAUSE, STREAM_END, STREAM_CLOSE, STREAM_DESTROY)) {
          remote.send(Message.Stream(id, mask or event, null, null))
        }
        remote.send(Message.Stream(id, mask or STREAM_DATA, "late".encodeToByteArray(), null))
      }
    }
    assertTrue(response(1u, "hi") in sent)
    assertEquals(0, rpc.tracked())
    remote.send(Message.Request(3u, 7u, 0u, "ok".encodeToByteArray()))
    rpc.assertOpen()
  }

  @Test
  fun `streams the peer ends without a close leave nothing tracked`() = runTest {
    val sent = mutableListOf<String>()
    val (remote, rpc) =
      connect(sent) { request ->
        Reply.Data(request.body!!.toList().fold(ByteArray(0), ByteArray::plus))
      }
    remote.send(Message.Request(1u, 7u, STREAM_OPEN, null))
    remote.send(Message.Stream(1u, STREAM_REQUEST or STREAM_DATA, "hi".encodeToByteArray(), null))
    remote.send(Message.Stream(1u, STREAM_REQUEST or STREAM_END, null, null))
    assertTrue(response(1u, "hi") in sent)
    val chunks = async { rpc.requestFlow(7u, flowOf("hi".encodeToByteArray())).toList() }
    runCurrent()
    remote.send(Message.Stream(1u, STREAM_REQUEST or STREAM_OPEN, null, null))
    remote.send(Message.Response(1u, STREAM_OPEN, null, null))
    remote.send(Message.Stream(1u, STREAM_RESPONSE or STREAM_DATA, "ok".encodeToByteArray(), null))
    remote.send(Message.Stream(1u, STREAM_RESPONSE or STREAM_END, null, null))
    assertEquals(listOf("ok"), chunks.await().map { it.decodeToString() })
    runCurrent()
    assertEquals(0, rpc.tracked())
  }
}
