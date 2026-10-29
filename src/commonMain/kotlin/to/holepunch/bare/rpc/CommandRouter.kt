package to.holepunch.bare.rpc

/**
 * Sends each request and event to the handler registered for its command. Pass [dispatch] to [Rpc]
 * as its `onRequest` and `onEvent`, and register every handler before creating it.
 */
public class CommandRouter {
  private val requestHandlers = mutableMapOf<ULong, suspend (IncomingRequest) -> Reply>()
  private val eventHandlers = mutableMapOf<ULong, suspend (IncomingEvent) -> Unit>()

  /** Answers requests for [command] with [handler], in place of any handler registered before. */
  public fun onRequest(command: ULong, handler: suspend (IncomingRequest) -> Reply) {
    requestHandlers[command] = handler
  }

  /** Delivers events for [command] to [handler], in place of any handler registered before. */
  public fun onEvent(command: ULong, handler: suspend (IncomingEvent) -> Unit) {
    eventHandlers[command] = handler
  }

  /**
   * Answers [request] with the handler for its command.
   *
   * @throws RemoteException with code `ERR_UNKNOWN_COMMAND` if no handler is registered for it.
   */
  public suspend fun dispatch(request: IncomingRequest): Reply {
    val handler = requestHandlers[request.command] ?: throw unknownCommand(request.command)
    return handler(request)
  }

  /** Delivers [event] to the handler for its command, and drops it if there is none. */
  public suspend fun dispatch(event: IncomingEvent) {
    eventHandlers[event.command]?.invoke(event)
  }
}
