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

  class RemoteError(val message: String, val code: String, val errno: Long) : ResponseResult
}

internal class StreamMessage(
  override val id: ULong,
  val flags: ULong,
  val data: ByteArray?,
  val error: ResponseResult.RemoteError?,
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

private const val DATA = 0x10uL
private const val ERROR = 0x80uL

private val body: Codec<Message> =
  object : Codec<Message> {
    override fun preencode(state: State, value: Message) = write(value, state, sizing = true)

    override fun encode(state: State, value: Message) = write(value, state, sizing = false)

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
          if (flags and ERROR != 0uL) TODO("error struct")
          StreamMessage(id, flags, if (flags and DATA != 0uL) buffer.decode(state) else null, null)
        }
        else -> TODO("unknown type")
      }
    }

    private fun write(value: Message, state: State, sizing: Boolean) {
      fun <T> put(codec: Codec<T>, field: T) =
        if (sizing) codec.preencode(state, field) else codec.encode(state, field)

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
          if (value.flags and DATA != 0uL) put(buffer, value.data ?: ByteArray(0))
        }
      }
    }
  }
