package to.holepunch.bare.rpc

/**
 * A [Handler] that sends each request and event to the handler registered for its command. Pass it
 * to [Rpc] as its `handler`, and register every handler before creating the [Rpc].
 */
public class CommandRouter : Handler {
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
   * @throws RemoteException with code `UNKNOWN_COMMAND` if no handler is registered for it.
   */
  override suspend fun handle(request: IncomingRequest): Reply {
    val handler = requestHandlers[request.command] ?: throw unknownCommand(request.command)
    return handler(request)
  }

  /** Delivers [event] to the handler for its command, and drops it if there is none. */
  override suspend fun handle(event: IncomingEvent) {
    eventHandlers[event.command]?.invoke(event)
  }
}
