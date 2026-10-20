package to.holepunch.barerpc

import to.holepunch.compactencoding.Codec
import to.holepunch.compactencoding.DecodingException
import to.holepunch.compactencoding.State
import to.holepunch.compactencoding.bool
import to.holepunch.compactencoding.buffer
import to.holepunch.compactencoding.int
import to.holepunch.compactencoding.uint
import to.holepunch.compactencoding.uint32
import to.holepunch.compactencoding.utf8

internal sealed interface Message {
  val id: ULong

  class Request(
    override val id: ULong,
    val command: ULong,
    val stream: ULong,
    val data: ByteArray?,
  ) : Message

  class Response(
    override val id: ULong,
    val stream: ULong,
    val data: ByteArray?,
    val error: RemoteError?,
  ) : Message

  class Stream(
    override val id: ULong,
    val flags: ULong,
    val data: ByteArray?,
    val error: RemoteError?,
  ) : Message
}

internal class RemoteError(val message: String, val code: String, val errno: Long)

internal val frame: Codec<Message> =
  object : Codec<Message> {
    override fun preencode(state: State, value: Message) {
      uint32.preencode(state, 0u)
      body.preencode(state, value)
    }

    override fun encode(state: State, value: Message) {
      val prefix = state.start
      state.start += 4
      body.encode(state, value)
      val end = state.start
      state.start = prefix
      uint32.encode(state, (end - prefix - 4).toUInt())
      state.start = end
    }

    override fun decode(state: State): Message {
      val outerEnd = state.end
      try {
        val length = uint32.decode(state)
        if (length > state.remaining.toUInt()) {
          throw MalformedFrameException(
            "frame length $length exceeds the ${state.remaining} bytes remaining"
          )
        }
        val frameEnd = state.start + length.toInt()
        state.end = frameEnd
        val message = body.decode(state)
        state.start = frameEnd
        return message
      } catch (e: DecodingException) {
        throw MalformedFrameException(e.message.orEmpty(), e)
      } finally {
        state.end = outerEnd
      }
    }
  }

private const val REQUEST = 1uL
private const val RESPONSE = 2uL
private const val STREAM = 3uL

internal const val STREAM_OPEN = 0x1uL
internal const val STREAM_CLOSE = 0x2uL
internal const val STREAM_PAUSE = 0x4uL
internal const val STREAM_RESUME = 0x8uL
internal const val STREAM_DATA = 0x10uL
internal const val STREAM_END = 0x20uL
internal const val STREAM_DESTROY = 0x40uL
internal const val STREAM_ERROR = 0x80uL
internal const val STREAM_REQUEST = 0x100uL
internal const val STREAM_RESPONSE = 0x200uL

internal val streamEvents =
  listOf(
    STREAM_OPEN,
    STREAM_CLOSE,
    STREAM_PAUSE,
    STREAM_RESUME,
    STREAM_DATA,
    STREAM_END,
    STREAM_DESTROY,
  )

private val body: Codec<Message> =
  object : Codec<Message> {
    override fun preencode(state: State, value: Message) = write(value, state, Preencode)

    override fun encode(state: State, value: Message) = write(value, state, Encode)

    override fun decode(state: State): Message {
      val type = uint.decode(state)
      val id = uint.decode(state)
      return when (type) {
        REQUEST -> {
          val command = uint.decode(state)
          val stream = uint.decode(state)
          Message.Request(id, command, stream, if (stream == 0uL) buffer.decode(state) else null)
        }
        RESPONSE -> {
          val hasError = bool.decode(state)
          val stream = uint.decode(state)
          if (hasError) Message.Response(id, stream, null, remoteError.decode(state))
          else Message.Response(id, stream, if (stream == 0uL) buffer.decode(state) else null, null)
        }
        STREAM -> {
          val flags = uint.decode(state)
          val error = if (flags and STREAM_ERROR != 0uL) remoteError.decode(state) else null
          val data =
            if (error == null && flags and STREAM_DATA != 0uL) buffer.decode(state) else null
          Message.Stream(id, flags, data, error)
        }
        else -> throw UnknownMessageException(type)
      }
    }

    private fun write(value: Message, state: State, step: Step) {
      fun <T> put(codec: Codec<T>, field: T) = step.put(codec, state, field)

      when (value) {
        is Message.Request -> {
          put(uint, REQUEST)
          put(uint, value.id)
          put(uint, value.command)
          put(uint, value.stream)
          if (value.stream == 0uL) put(buffer, value.data ?: ByteArray(0))
        }
        is Message.Response -> {
          put(uint, RESPONSE)
          put(uint, value.id)
          val error = value.error
          put(bool, error != null)
          put(uint, value.stream)
          if (error != null) put(remoteError, error)
          else if (value.stream == 0uL) put(buffer, value.data ?: ByteArray(0))
        }
        is Message.Stream -> {
          put(uint, STREAM)
          put(uint, value.id)
          put(uint, value.flags)
          when {
            value.flags and STREAM_ERROR != 0uL -> put(remoteError, value.error!!)
            value.flags and STREAM_DATA != 0uL -> put(buffer, value.data ?: ByteArray(0))
          }
        }
      }
    }
  }

private val remoteError: Codec<RemoteError> =
  object : Codec<RemoteError> {
    override fun preencode(state: State, value: RemoteError) = write(value, state, Preencode)

    override fun encode(state: State, value: RemoteError) = write(value, state, Encode)

    override fun decode(state: State): RemoteError {
      val message = utf8.decode(state)
      val code = utf8.decode(state)
      val errno = int.decode(state)
      return RemoteError(message, code, errno)
    }

    private fun write(value: RemoteError, state: State, step: Step) {
      fun <T> put(codec: Codec<T>, field: T) = step.put(codec, state, field)

      put(utf8, value.message)
      put(utf8, value.code)
      put(int, value.errno)
    }
  }

private interface Step {
  fun <T> put(codec: Codec<T>, state: State, value: T)
}

private object Preencode : Step {
  override fun <T> put(codec: Codec<T>, state: State, value: T) = codec.preencode(state, value)
}

private object Encode : Step {
  override fun <T> put(codec: Codec<T>, state: State, value: T) = codec.encode(state, value)
}
