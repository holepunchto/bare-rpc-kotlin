package to.holepunch.barerpc.usage

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import to.holepunch.barerpc.BareRpc
import to.holepunch.barerpc.CommandRouter
import to.holepunch.barerpc.Reply

class UsageTest {
  @Test
  fun `a request is answered by the other end`() = runTest {
    val toServer = Channel<ByteArray>(Channel.UNLIMITED)
    val toClient = Channel<ByteArray>(Channel.UNLIMITED)

    val router = CommandRouter()
    router.onRequest(1u) { request ->
      Reply.Data("hello ${request.data.decodeToString()}".encodeToByteArray())
    }

    val server =
      BareRpc(send = toClient::send, onRequest = router::dispatch, onEvent = router::dispatch)
    val client = BareRpc(send = toServer::send)

    launch { for (bytes in toServer) server.receive(bytes) }
    launch { for (bytes in toClient) client.receive(bytes) }

    assertEquals("hello ada", client.request(1u, "ada".encodeToByteArray()).decodeToString())
    coroutineContext.cancelChildren()
  }
}
