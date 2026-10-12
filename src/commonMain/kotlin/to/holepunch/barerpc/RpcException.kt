package to.holepunch.barerpc

/** A failure of an RPC exchange, raised by this side or reported by the peer. */
public sealed class RpcException(message: String, cause: Throwable? = null) :
  Exception(message, cause)

/**
 * A frame names a message type the wire format does not define.
 *
 * @property type the type the frame names.
 */
public class UnknownMessageException internal constructor(public val type: ULong) :
  RpcException("Unknown message '$type'")

/**
 * A frame cannot be read: its bytes end before its length prefix or the length it declares, or a
 * field runs past that length.
 */
public class MalformedFrameException
internal constructor(message: String, cause: Throwable? = null) : RpcException(message, cause)
