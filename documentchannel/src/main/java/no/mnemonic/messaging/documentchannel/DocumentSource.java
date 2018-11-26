package no.mnemonic.messaging.documentchannel;

/**
 * A configured document source, which represents a session for this channel.
 * A client can subscribe to this channel by creating a document channel subscription
 * using {@link #createDocumentSubscription(DocumentChannelListener)}
 *
 * @param <T> document type
 */
public interface DocumentSource<T> extends AutoCloseable {

  /**
   * Create a subscription, which will submit incoming documents to the listener
   * @param listener listener to send documents to
   * @return a subscription object, which handles the subscription state
   */
  DocumentChannelSubscription createDocumentSubscription(DocumentChannelListener<T> listener);

  /**
   * Close this source session
   */
  @Override
  void close();

}
