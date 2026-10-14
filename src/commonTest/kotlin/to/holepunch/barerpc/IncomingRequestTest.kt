package to.holepunch.barerpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.test.runTest

class IncomingRequestTest {
  private val hi = "0700000001050700026869" // request id 5, command 7, "hi"
  private val ok = "0700000002050000026f6b" // response id 5, "ok"
  private val boom = "110000000205010004626f6f6d06455f424f4f4d02" // error id 5: "boom", "E_BOOM", 1
  private val unknown =
    "2b0000000205010011556e6b6e6f776e20636f6d6d616e642037134552525f554e4b4e4f574e5f434f4d4d414e4400"
  private val secret = "0d00000002050100067365637265740000" // error id 5: "secret", "", 0

  @Test
  fun `a request reaches the handler and its return value goes back`() = runTest {
    val sent = mutableListOf<String>()
    val seen = mutableListOf<Pair<ULong, String>>()
    val rpc =
      BareRpc(
        send = { sent += it.toHexString() },
        onRequest = {
          seen += it.command to it.data.decodeToString()
          "ok".encodeToByteArray()
        },
      )
    rpc.receive(hi.hexToByteArray())
    assertEquals(listOf(7uL to "hi"), seen)
    assertEquals(listOf(ok), sent)
  }

  @Test
  fun `any other exception from the handler goes back with its message`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() }, onRequest = { error("secret") })
    rpc.receive(hi.hexToByteArray())
    assertEquals(listOf(secret), sent)
  }

  @Test
  fun `a request with no handler is rejected as an unknown command`() = runTest {
    val sent = mutableListOf<String>()
    BareRpc(send = { sent += it.toHexString() }).receive(hi.hexToByteArray())
    assertEquals(listOf(unknown), sent)
  }

  @Test
  fun `a throwing handler leaves the connection open`() = runTest {
    val sent = mutableListOf<String>()
    val rpc = BareRpc(send = { sent += it.toHexString() }, onRequest = { error("secret") })
    rpc.receive(hi.hexToByteArray())
    rpc.receive(hi.hexToByteArray())
    assertEquals(listOf(secret, secret), sent)
  }
}
