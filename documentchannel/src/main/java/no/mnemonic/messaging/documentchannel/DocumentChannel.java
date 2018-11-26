package no.mnemonic.messaging.documentchannel;

/**
 * A writer interface to put documents to.
 *
 * @param <T> document type
 */
public interface DocumentChannel<T> {

  /**
   * Submit a new document. The document will be delivered to any configured document channel subscribers
   * for this channel.
   *
   * @param document the document channel
   */
  void sendDocument(T document);

}
