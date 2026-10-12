package to.holepunch.barerpc

import java.io.File
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

internal val fixtures = File(System.getProperty("hrpc-test.fixtures"))

internal val directories = fixtures.listFiles()!!.filter { it.isDirectory }.map { it.name }.toSet()

internal val supported = setOf("envelope", "error")

internal val unsupported = setOf("boundary", "dispatch", "dispatch-sparse", "negative", "sequence")

internal class Case(val note: String, val descriptor: JsonObject, val frame: ByteArray)

internal fun cases(family: String): List<Case> {
  val messages = read(family, "messages.json").jsonArray
  val frames = read(family, "frames.json").jsonArray
  check(messages.size == frames.size) {
    "$family: ${messages.size} messages, ${frames.size} frames"
  }
  return messages.zip(frames) { message, frame ->
    Case(
      message.jsonObject.getValue("note").jsonPrimitive.content,
      message.jsonObject.getValue("descriptor").jsonObject,
      frame.jsonPrimitive.content.hexToByteArray(),
    )
  }
}

internal fun messageOf(descriptor: JsonObject): Message {
  val id = descriptor.uint("id")
  val stream = descriptor.uint("stream")
  val data = descriptor.getValue("data").let { if (it is JsonNull) null else it.hex() }
  return when (descriptor.uint("type")) {
    1uL -> RequestMessage(id, descriptor.uint("command"), stream, data)
    2uL -> {
      val error = descriptor.remoteError()
      check(error == null || data == null) { "response with both error and data" }
      ResponseMessage(id, stream, error ?: ResponseResult.Success(data))
    }
    3uL -> StreamMessage(id, stream, data, descriptor.remoteError())
    else -> error("type ${descriptor["type"]}")
  }
}

internal fun descriptorOf(message: Message): JsonObject =
  when (message) {
    is RequestMessage ->
      JsonObject(
        mapOf(
          "type" to JsonPrimitive(1),
          "id" to message.id.json(),
          "command" to message.command.json(),
          "stream" to message.stream.json(),
          "data" to message.data.json(),
        )
      )
    is ResponseMessage ->
      JsonObject(
        mapOf(
          "type" to JsonPrimitive(2),
          "id" to message.id.json(),
          "stream" to message.stream.json(),
          "error" to (message.result as? RemoteError).json(),
          "data" to (message.result as? ResponseResult.Success)?.data.json(),
        )
      )
    is StreamMessage ->
      JsonObject(
        mapOf(
          "type" to JsonPrimitive(3),
          "id" to message.id.json(),
          "stream" to message.flags.json(),
          "error" to message.error.json(),
          "data" to message.data.json(),
        )
      )
  }

private fun read(family: String, name: String): JsonElement =
  Json.parseToJsonElement(File(fixtures, "$family/$name").readText())

private fun JsonObject.remoteError(): RemoteError? =
  (getValue("error") as? JsonObject)?.let {
    RemoteError(
      it.getValue("message").jsonPrimitive.content,
      it.getValue("code").jsonPrimitive.content,
      it.getValue("errno").jsonPrimitive.long,
    )
  }

private fun JsonObject.uint(key: String): ULong = getValue(key).jsonPrimitive.long.toULong()

private fun JsonElement.hex(): ByteArray = jsonPrimitive.content.hexToByteArray()

private fun ULong.json(): JsonElement = JsonPrimitive(toLong())

private fun RemoteError?.json(): JsonElement =
  if (this == null) JsonNull
  else
    JsonObject(
      mapOf(
        "message" to JsonPrimitive(message),
        "code" to JsonPrimitive(code),
        "errno" to JsonPrimitive(errno),
      )
    )

private fun ByteArray?.json(): JsonElement =
  if (this == null) JsonNull else JsonPrimitive(toHexString())
