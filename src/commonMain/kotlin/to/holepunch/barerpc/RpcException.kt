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
 * A frame cannot be read: its bytes end before its length prefix or the length it declares, a field
 * runs past that length, or the frame is longer than the cap.
 */
public class MalformedFrameException
internal constructor(message: String, cause: Throwable? = null) : RpcException(message, cause)

/**
 * The peer replied to a request with an error, or a request handler answers with one.
 *
 * @property code the error's code, such as `ERR_UNKNOWN_COMMAND`.
 * @property errno the error's number.
 */
public class RemoteException(message: String, public val code: String, public val errno: Long) :
  RpcException(message)

/** The peer replied with a stream to a request that expected a single reply. */
public class UnexpectedStreamException internal constructor() :
  RpcException("The peer replied with a stream")

/** The peer sent more of a stream than [BareRpc] buffers for it, past its pause. */
public class BufferOverflowException internal constructor() :
  RpcException("A stream buffered past its cap")

/** The connection was closed by [BareRpc.close]. */
public class ConnectionClosedException internal constructor() : RpcException("Connection closed")

internal fun remoteException(error: RemoteError) =
  RemoteException(error.message, error.code, error.errno)

internal fun noScope() = RemoteException("Request streams need a scope", "ERR_NO_SCOPE", 0)

internal fun unknownCommand(command: ULong) =
  RemoteException("Unknown command $command", "ERR_UNKNOWN_COMMAND", 0)
