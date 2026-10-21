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

/** One bare-rpc message, the body of a frame. */
public sealed interface Message {
  /** The request id, or 0 for an event. */
  public val id: ULong

  /**
   * A request or, with an [id] of 0, an event.
   *
   * @property command the command the request names.
   * @property stream the stream flags, 0 unless the request opens a stream.
   * @property data the payload, absent when the request opens a stream.
   */
  public class Request(
    override val id: ULong,
    public val command: ULong,
    public val stream: ULong,
    public val data: ByteArray?,
  ) : Message {
    /** Equal when every field is, comparing [data] by content. */
    override fun equals(other: Any?): Boolean =
      other is Request &&
        id == other.id &&
        command == other.command &&
        stream == other.stream &&
        data.contentEquals(other.data)

    /** Consistent with [equals]. */
    override fun hashCode(): Int = listOf(id, command, stream, data.contentHashCode()).hashCode()
  }

  /**
   * A reply to the request with the same [id].
   *
   * @property stream the stream flags, 0 unless the reply opens a stream.
   * @property data the payload, absent when the reply is an error or opens a stream.
   * @property error the error the request failed with, or null.
   */
  public class Response(
    override val id: ULong,
    public val stream: ULong,
    public val data: ByteArray?,
    public val error: RemoteError?,
  ) : Message {
    /** Equal when every field is, comparing [data] by content. */
    override fun equals(other: Any?): Boolean =
      other is Response &&
        id == other.id &&
        stream == other.stream &&
        data.contentEquals(other.data) &&
        error == other.error

    /** Consistent with [equals]. */
    override fun hashCode(): Int = listOf(id, stream, data.contentHashCode(), error).hashCode()
  }

  /**
   * A frame of a request or reply stream.
   *
   * @property flags the stream event and the direction it travels in.
   * @property data the payload of a data frame, or null.
   * @property error the error a stream is closed with, or null.
   */
  public class Stream(
    override val id: ULong,
    public val flags: ULong,
    public val data: ByteArray?,
    public val error: RemoteError?,
  ) : Message {
    /** Equal when every field is, comparing [data] by content. */
    override fun equals(other: Any?): Boolean =
      other is Stream &&
        id == other.id &&
        flags == other.flags &&
        data.contentEquals(other.data) &&
        error == other.error

    /** Consistent with [equals]. */
    override fun hashCode(): Int = listOf(id, flags, data.contentHashCode(), error).hashCode()
  }
}

/**
 * The error a peer sends in place of a reply or to close a stream.
 *
 * @property message the error's message.
 * @property code the error's code, such as `ERR_UNKNOWN_COMMAND`.
 * @property errno the error's number.
 */
public data class RemoteError(val message: String, val code: String, val errno: Long)

/**
 * The result of [Messages.decode].
 *
 * @property message the decoded message.
 * @property read the number of bytes the frame took, length prefix included.
 */
public class Decoded(public val message: Message, public val read: Int)

/** The bare-rpc frame codec: a uint32 little-endian length followed by the message. */
public object Messages {
  /**
   * Encodes [message] as one frame.
   *
   * @throws IllegalArgumentException for a [Message.Stream] whose flags carry the error flag
   *   without an [Message.Stream.error].
   */
  public fun encode(message: Message): ByteArray =
    to.holepunch.compactencoding.encode(frame, message)

  /**
   * Decodes the frame at the start of [bytes].
   *
   * @throws MalformedFrameException when the frame is truncated or its body cannot be read.
   * @throws UnknownMessageException when the message type is not one this format defines.
   */
  public fun decode(bytes: ByteArray): Decoded {
    val state = State(bytes)
    val message = frame.decode(state)
    return Decoded(message, state.start)
  }
}

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
            value.flags and STREAM_ERROR != 0uL ->
              put(remoteError, requireNotNull(value.error) { "the error flag needs an error" })
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
