package to.holepunch.bare.rpc

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield

class RequestStreamTest {
  private val open = "0400000001010701" // request id 1, command 7, opens a stream
  private val ack = "050000000301fd0101" // stream id 1, request open
  private val data = "080000000301fd1001026869" // stream id 1, request data "hi"
  private val end = "050000000301fd2001" // stream id 1, request end
  private val pause = "050000000301fd0401" // stream id 1, request pause
  private val resume = "050000000301fd0801" // stream id 1, request resume
  // stream id 1, request close error: "boom", "E_BOOM", errno 1
  private val closeError = "120000000301fd820104626f6f6d06455f424f4f4d02"
  private val destroy = "050000000301fd4001" // stream id 1, request destroy
  // stream id 1, request destroy error: "boom", "E_BOOM", errno 1
  private val destroyError = "120000000301fdc00104626f6f6d06455f424f4f4d02"
  private val ok = "0700000002010000026f6b" // response id 1, "ok"
  private val boom =
    "110000000201010004626f6f6d06455f424f4f4d02" // error id 1: "boom", "E_BOOM", errno 1

  @Test
  fun `a request stream opens and waits for the ack before sending data`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
    launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    assertEquals(listOf(open), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `after the ack the body goes out as data then end`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
    launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send(ack.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `a request stream returns the reply`() = runTest {
    val remote = Remote(this)
    val rpc = Rpc(remote.data, write = {}, scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send(ack.hexToByteArray())
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `an error response before the ack throws and sends no body`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val sent = mutableListOf<String>()
      val rpc = Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      remote.send(boom.hexToByteArray())
      assertEquals(listOf(open), sent)
      assertFailsWith<RemoteException> { reply.await() }
    }
  }

  @Test
  fun `close while waiting for the ack throws ConnectionClosedException`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(remote.data, write = {}, scope = backgroundScope)
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      rpc.close()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `a reply before the ack skips the body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
    assertEquals(listOf(open), sent)
  }

  @Test
  fun `a pause holds the request body until resume`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
    launch { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send((ack + pause).hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    remote.send(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open, data, end), sent)
    coroutineContext.cancelChildren()
  }

  @Test
  fun `close releases a paused request body`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val rpc = Rpc(remote.data, write = {}, scope = backgroundScope)
      val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
      yield()
      remote.send((ack + pause).hexToByteArray())
      yield()
      rpc.close()
      assertFailsWith<ConnectionClosedException> { reply.await() }
    }
  }

  @Test
  fun `a reply stops a paused request body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send((ack + pause).hexToByteArray())
    yield()
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
    assertEquals(listOf(open), sent)
  }

  @Test
  fun `a destroy stops the request body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send((ack + pause).hexToByteArray())
    yield()
    remote.send(destroy.hexToByteArray())
    remote.send(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `a destroy with an error stops the request body`() = runTest {
    val remote = Remote(this)
    val sent = mutableListOf<String>()
    val rpc = Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
    val reply = async { rpc.request(7u, flowOf("hi".encodeToByteArray())) }
    yield()
    remote.send((ack + pause).hexToByteArray())
    yield()
    remote.send(destroyError.hexToByteArray())
    remote.send(resume.hexToByteArray())
    yield()
    assertEquals(listOf(open), sent)
    remote.send(ok.hexToByteArray())
    assertEquals("ok", reply.await().decodeToString())
  }

  @Test
  fun `a throwing request body closes the stream with the error`() = runTest {
    val remote = Remote(this)
    supervisorScope {
      val sent = mutableListOf<String>()
      val rpc = Rpc(remote.data, write = { sent += it.toHexString() }, scope = backgroundScope)
      val body = flow<ByteArray> { throw RemoteException("boom", "E_BOOM", 1) }
      val reply = async { rpc.request(7u, body) }
      yield()
      remote.send(ack.hexToByteArray())
      yield()
      assertEquals("E_BOOM", assertFailsWith<RemoteException> { reply.await() }.code)
      assertEquals(listOf(open, closeError), sent)
    }
  }
}
