package to.holepunch.bare.rpc

/**
 * Answers the requests and receives the events of the peer of an [Rpc].
 *
 * Both functions have defaults, so an implementation can handle only one kind. By default, requests
 * are rejected as an unknown command and events are dropped.
 */
public interface Handler {
  /**
   * Answers [request]: the return value is the reply, and an exception thrown goes back to the peer
   * as an error, a [RemoteException] as itself.
   *
   * @throws RemoteException with code `UNKNOWN_COMMAND` by default.
   */
  public suspend fun handle(request: IncomingRequest): Reply = throw unknownCommand(request.command)

  /** Receives [event]. An exception thrown fails the session. By default, the event is dropped. */
  public suspend fun handle(event: IncomingEvent) {}
}

internal class FunctionHandler(
  private val onRequest: suspend (IncomingRequest) -> Reply,
  private val onEvent: suspend (IncomingEvent) -> Unit,
) : Handler {
  override suspend fun handle(request: IncomingRequest): Reply = onRequest(request)

  override suspend fun handle(event: IncomingEvent): Unit = onEvent(event)
}
