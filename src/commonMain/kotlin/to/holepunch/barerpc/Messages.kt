package to.holepunch.barerpc

import to.holepunch.compactencoding.Codec
import to.holepunch.compactencoding.State
import to.holepunch.compactencoding.bool
import to.holepunch.compactencoding.buffer
import to.holepunch.compactencoding.uint
import to.holepunch.compactencoding.uint32

internal sealed interface Message {
  val id: ULong
}

internal class RequestMessage(
  override val id: ULong,
  val command: ULong,
  val stream: ULong,
  val data: ByteArray?,
) : Message

internal class ResponseMessage(
  override val id: ULong,
  val stream: ULong,
  val result: ResponseResult,
) : Message

internal sealed interface ResponseResult {
  class Success(val data: ByteArray?) : ResponseResult
}

internal class RemoteError(val message: String, val code: String, val errno: Long) : ResponseResult

internal class StreamMessage(
  override val id: ULong,
  val flags: ULong,
  val data: ByteArray?,
  val error: RemoteError?,
) : Message

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
      uint32.decode(state)
      return body.decode(state)
    }
  }

private const val REQUEST = 1uL
private const val RESPONSE = 2uL
private const val STREAM = 3uL

private const val STREAM_DATA = 0x10uL
private const val STREAM_ERROR = 0x80uL

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
          RequestMessage(id, command, stream, if (stream == 0uL) buffer.decode(state) else null)
        }
        RESPONSE -> {
          if (bool.decode(state)) TODO("error struct")
          val stream = uint.decode(state)
          ResponseMessage(
            id,
            stream,
            ResponseResult.Success(if (stream == 0uL) buffer.decode(state) else null),
          )
        }
        STREAM -> {
          val flags = uint.decode(state)
          if (flags and STREAM_ERROR != 0uL) TODO("error struct")
          StreamMessage(
            id,
            flags,
            if (flags and STREAM_DATA != 0uL) buffer.decode(state) else null,
            null,
          )
        }
        else -> TODO("unknown type")
      }
    }

    private fun write(value: Message, state: State, step: Step) {
      fun <T> put(codec: Codec<T>, field: T) = step.put(codec, state, field)

      when (value) {
        is RequestMessage -> {
          put(uint, REQUEST)
          put(uint, value.id)
          put(uint, value.command)
          put(uint, value.stream)
          if (value.stream == 0uL) put(buffer, value.data ?: ByteArray(0))
        }
        is ResponseMessage -> {
          put(uint, RESPONSE)
          put(uint, value.id)
          val data = (value.result as? ResponseResult.Success ?: TODO("error struct")).data
          put(bool, false)
          put(uint, value.stream)
          if (value.stream == 0uL) put(buffer, data ?: ByteArray(0))
        }
        is StreamMessage -> {
          put(uint, STREAM)
          put(uint, value.id)
          put(uint, value.flags)
          if (value.error != null) TODO("error struct")
          if (value.flags and STREAM_DATA != 0uL) put(buffer, value.data ?: ByteArray(0))
        }
      }
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
