package to.holepunch.bare.rpc

/** A failure of an RPC exchange, raised by this side or reported by the peer. */
public sealed class RpcException(message: String, cause: Throwable? = null) :
  Exception(message, cause)

/**
 * A frame's length prefix declares more than `maxFrameSize` allows. It is checked before the rest
 * of the frame is read.
 *
 * @property size the frame's length, length prefix included.
 * @property limit the `maxFrameSize` it exceeds.
 */
public class FrameTooLargeException
internal constructor(public val size: Long, public val limit: Int) :
  RpcException("Frame of $size bytes exceeds the limit of $limit bytes")

/**
 * A frame names a message type the wire format does not define.
 *
 * @property type the type the frame names.
 */
public class UnknownMessageException internal constructor(public val type: ULong) :
  RpcException("Unknown message '$type'")

/**
 * A frame cannot be read: its bytes end before its length prefix or the length it declares, a field
 * runs past that length, a response's error flag is neither 0 nor 1, or the data ends partway
 * through a frame. Or the frame is one that a valid peer never sends: a response or stream frame
 * without a request id, a stream frame whose flags are not one direction and one event, stream
 * flags that a request or response cannot carry, a request with the id of one that is still in
 * progress, or acks for more than 1024 replies that have yet to be sent.
 */
public class MalformedFrameException
internal constructor(message: String, cause: Throwable? = null) : RpcException(message, cause)

/**
 * The peer replied to a request with an error, or a request handler answers with one.
 *
 * @property code the error's code, such as `UNKNOWN_COMMAND`.
 * @property errno the error's number.
 */
public class RemoteException(message: String, public val code: String, public val errno: Long) :
  RpcException(message)

/** The peer replied with a stream to a request that expected a single reply. */
public class UnexpectedStreamException internal constructor() :
  RpcException("The peer replied with a stream")

/** The peer sent more of one stream than `maxBufferedBytes` allows. */
public class StreamOverflowException internal constructor() :
  RpcException("A stream buffered past its cap")

/** A request body was collected a second time. A body can be collected only once. */
public class AlreadyReceivedException internal constructor() :
  RpcException("The request body has already been received")

/** The connection was closed, by [Rpc.close] or by the end of the data received from the peer. */
public class ConnectionClosedException internal constructor() : RpcException("Connection closed")

internal fun remoteException(error: RemoteError) =
  RemoteException(error.message, error.code, error.errno)

internal fun unknownCommand(command: ULong) =
  RemoteException("Unknown command $command", "UNKNOWN_COMMAND", 0)
