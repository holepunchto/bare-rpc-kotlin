package to.holepunch.barerpc

import kotlinx.coroutines.flow.Flow

/** What a request handler answers with. */
public sealed interface Reply {
  /**
   * A reply sent at once.
   *
   * @property data the reply's payload.
   */
  public class Data(public val data: ByteArray) : Reply

  /**
   * A reply streamed to the peer once it accepts the stream.
   *
   * @property body the reply's streamed payload.
   */
  public class Stream(public val body: Flow<ByteArray>) : Reply
}
