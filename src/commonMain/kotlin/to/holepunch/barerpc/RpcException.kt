package to.holepunch.barerpc

/** A failure of an RPC exchange, raised by this side or reported by the peer. */
public sealed class RpcException(message: String) : Exception(message)

/**
 * A frame names a message type the wire format does not define.
 *
 * @property type the type the frame names.
 */
public class UnknownMessageException internal constructor(public val type: ULong) :
  RpcException("Unknown message '$type'")
