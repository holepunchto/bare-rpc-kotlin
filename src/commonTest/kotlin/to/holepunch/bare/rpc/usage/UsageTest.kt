package to.holepunch.bare.rpc.usage

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import to.holepunch.bare.rpc.CommandRouter
import to.holepunch.bare.rpc.Reply
import to.holepunch.bare.rpc.Rpc

class UsageTest {
  @Test
  fun `a request is answered by the other end`() = runTest {
    val toServer = Channel<ByteArray>(Channel.UNLIMITED)
    val toClient = Channel<ByteArray>(Channel.UNLIMITED)

    val router = CommandRouter()
    router.onRequest(1u) { request ->
      Reply.Data("hello ${request.data.decodeToString()}".encodeToByteArray())
    }

    Rpc(
      toServer.consumeAsFlow(),
      toClient::send,
      backgroundScope,
      onRequest = router::dispatch,
      onEvent = router::dispatch,
    )
    val client = Rpc(toClient.consumeAsFlow(), toServer::send, backgroundScope)

    assertEquals("hello ada", client.request(1u, "ada".encodeToByteArray()).decodeToString())
  }

  @Test
  fun `a streamed request is answered with a streamed reply`() = runTest {
    val toServer = Channel<ByteArray>(Channel.UNLIMITED)
    val toClient = Channel<ByteArray>(Channel.UNLIMITED)

    val router = CommandRouter()
    router.onRequest(2u) { request ->
      Reply.Stream(request.body!!.map { it.decodeToString().uppercase().encodeToByteArray() })
    }

    Rpc(toServer.consumeAsFlow(), toClient::send, backgroundScope, onRequest = router::dispatch)
    val client = Rpc(toClient.consumeAsFlow(), toServer::send, backgroundScope)

    val body = flowOf("ada", "grace").map { it.encodeToByteArray() }
    assertEquals(
      listOf("ADA", "GRACE"),
      client.requestFlow(2u, body).toList().map { it.decodeToString() },
    )
  }
}
