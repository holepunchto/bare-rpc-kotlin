package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.toList

class InteropTest {
  @Test
  fun `a request to the JS peer comes back`() = withPeer { rpc ->
    assertEquals("hi", rpc.request(0u, "hi".encodeToByteArray()).decodeToString())
  }

  @Test
  fun `an error from the JS peer arrives as a remote exception`() = withPeer { rpc ->
    val error = assertFailsWith<RemoteException> { rpc.request(1u, ByteArray(0)) }
    assertEquals(Triple("boom", "E_PEER", 7L), Triple(error.message, error.code, error.errno))
  }

  @Test
  fun `a request from the JS peer reaches the Kotlin handler`() =
    withPeer(
      onRequest = { Reply.Data("${it.command}:${it.data.decodeToString()}".encodeToByteArray()) }
    ) { rpc ->
      assertEquals("7:hi", rpc.request(2u, "hi".encodeToByteArray()).decodeToString())
    }

  @Test
  fun `an event to the JS peer comes back as an event`() {
    val events = Channel<String>(1)
    withPeer(onEvent = { events.send("${it.command}:${it.data.decodeToString()}") }) { rpc ->
      rpc.event(3u, "hi".encodeToByteArray())
      assertEquals("3:hi", events.receive())
    }
  }

  @Test
  fun `a command Kotlin does not know goes back to the JS peer as an error`() = withPeer { rpc ->
    val error = assertFailsWith<RemoteException> { rpc.request(2u, ByteArray(0)) }
    assertEquals(
      Triple("Unknown command 7", "ERR_UNKNOWN_COMMAND", 0L),
      Triple(error.message, error.code, error.errno),
    )
  }

  @Test
  fun `a request stream reaches the JS peer`() = withPeer { rpc ->
    assertEquals(
      "hihi",
      rpc
        .request(
          4u,
          flowOf(
            "hi".encodeToByteArray(),
            "hi".encodeToByteArray(),
          ),
        )
        .decodeToString(),
    )
  }

  @Test
  fun `a large request stream chunk reaches the JS peer`() = withPeer { rpc ->
    val reply = rpc.request(4u, flowOf(ByteArray(32 * 1024) { 1 }))
    assertEquals(reply.size, 32 * 1024)
  }

  @Test
  fun `a request stream from the JS peer reaches the Kotlin handler`() =
    withPeer(onRequest = { Reply.Data(it.body!!.fold(ByteArray(0)) { a, b -> a + b }) }) { rpc ->
      assertEquals("hi", rpc.request(5u, "hi".encodeToByteArray()).decodeToString())
    }

  @Test
  fun `a streamed reply reaches the JS peer`() =
    withPeer(onRequest = { Reply.Stream(flowOf(it.data, it.data)) }) { rpc ->
      assertEquals("hihi", rpc.request(6u, "hi".encodeToByteArray()).decodeToString())
    }

  @Test
  fun `a streamed reply from the JS peer is collected`() = withPeer { rpc ->
    assertEquals(
      "hihi",
      rpc
        .requestFlow(7u, "hi".encodeToByteArray())
        .fold(ByteArray(0)) { a, b -> a + b }
        .decodeToString(),
    )
  }

  @Test
  fun `a request stream echoed by the JS peer comes back as a stream`() = withPeer { rpc ->
    assertEquals(
      listOf("a", "b"),
      rpc.requestFlow(8u, flowOf("a".encodeToByteArray(), "b".encodeToByteArray())).toList().map {
        it.decodeToString()
      },
    )
  }
}
