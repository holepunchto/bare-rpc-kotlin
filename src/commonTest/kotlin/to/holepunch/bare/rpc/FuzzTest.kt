package to.holepunch.bare.rpc

import kotlin.coroutines.cancellation.CancellationException
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import to.holepunch.compactEncoding.encode

/**
 * A peer that sends random frames. A careful one sends only what a valid peer could, references to
 * finished or unknown requests included, while a hostile one also breaks and mangles its frames.
 */
private class FuzzPeer(private val random: Random, private val hostile: Boolean) {
  private var lastId = 0uL
  private var breaking = false
  private val error = RemoteError("boom", "E_BOOM", 1)

  private fun reference(): ULong =
    when {
      breaking && random.nextInt(8) == 0 -> 0uL
      random.nextInt(4) == 0 -> random.nextLong(1, 1L shl 20).toULong()
      else -> random.nextLong(1, lastId.toLong() + 4).toULong()
    }

  private fun requestId(): ULong = if (breaking) random.nextLong(0, 8).toULong() else ++lastId

  private fun stream(): ULong =
    if (breaking && random.nextInt(4) == 0) random.nextLong(0, 0x400).toULong()
    else streamFlags.random(random)

  private fun message(): Message {
    val data = random.nextBytes(random.nextInt(0, 8))
    return when (random.nextInt(3)) {
      0 -> {
        val stream =
          if (breaking && random.nextInt(4) == 0) 2uL else listOf(0uL, STREAM_OPEN).random(random)
        Message.Request(requestId(), random.nextLong(0, 4).toULong(), stream, data)
      }
      1 ->
        when {
          breaking && random.nextInt(4) == 0 -> Message.Response(reference(), 2uL, data, error)
          random.nextBoolean() -> Message.Response(reference(), 0uL, null, error)
          else -> Message.Response(reference(), listOf(0uL, STREAM_OPEN).random(random), data, null)
        }
      else -> {
        val flags = stream()
        Message.Stream(reference(), flags, data, if (flags and STREAM_ERROR != 0uL) error else null)
      }
    }
  }

  // Keeps the length prefix whole, so that a frame never swallows the ones after it.
  fun frame(): ByteArray {
    breaking = hostile && random.nextInt(32) == 0
    val encoded = encode(frame, message())
    if (!breaking) return encoded
    var body =
      when (random.nextInt(10)) {
        0 -> random.nextBytes(random.nextInt(0, 16))
        1 -> return byteArrayOf(0xff.toByte(), 0xff.toByte(), 0, 0)
        else -> encoded.copyOfRange(4, encoded.size)
      }
    if (body.isNotEmpty() && random.nextInt(4) == 0) {
      body[random.nextInt(body.size)] = random.nextInt(256).toByte()
    }
    if (body.isNotEmpty() && random.nextInt(4) == 0)
      body = body.copyOfRange(0, random.nextInt(body.size))
    return byteArrayOf(body.size.toByte(), (body.size shr 8).toByte(), 0, 0) + body
  }
}

@OptIn(ExperimentalCoroutinesApi::class)
class FuzzTest {
  @Test
  fun `random and mutated frames fail the connection cleanly or leave it working`() = runTest {
    for (seed in 0 until 400) {
      val hostile = seed % 2 == 1
      val peer = FuzzPeer(Random(seed), hostile)
      val scope = CoroutineScope(backgroundScope.coroutineContext + Job())
      val remote = Remote(this)
      val sent = mutableListOf<ByteArray>()
      val thrown = mutableListOf<Throwable>()
      val rpc =
        Rpc(
          TestTransport(remote.data, { sent += it }),
          onRequest = {
            when (it.command) {
              0uL -> Reply.Data(it.data)
              1uL -> Reply.Stream(flowOf(byteArrayOf(1), byteArrayOf(2)))
              2uL -> {
                it.body?.collect()
                Reply.Data(ByteArray(0))
              }
              else -> {
                if (it.body != null) awaitCancellation()
                Reply.Data(ByteArray(0))
              }
            }
          },
          maxFrameSize = 256,
          scope = scope,
        )
      for (call in
        listOf<suspend () -> Unit>(
          { rpc.request(1u, flowOf(byteArrayOf(1))) },
          { rpc.requestFlow(2u, ByteArray(0)).collect() },
        )) {
        scope.launch {
          try {
            call()
          } catch (e: Throwable) {
            thrown += e
          }
        }
      }
      runCurrent()
      var frames = 0
      repeat(40) {
        val count = Random(seed * 31 + it).nextInt(1, 4)
        frames += count
        remote.send((0 until count).fold(ByteArray(0)) { chunk, _ -> chunk + peer.frame() })
      }
      val failed =
        try {
          rpc.assertOpen()
          false
        } catch (e: RpcException) {
          true
        }
      if (!hostile) assertFalse(failed, "seed $seed")
      assertTrue(rpc.tracked() <= 1024 + 5 * frames, "seed $seed")
      if (!failed) {
        val id = 1uL shl 40
        remote.send(Message.Request(id, 0u, 0u, "ok".encodeToByteArray()))
        val reply = encode(frame, Message.Response(id, 0u, "ok".encodeToByteArray(), null))
        assertTrue(sent.any { it.contentEquals(reply) }, "seed $seed")
      }
      scope.cancel()
      runCurrent()
      for (e in thrown) assertTrue(
        e is RpcException || e is CancellationException,
        "seed $seed: $e",
      )
    }
  }
}
