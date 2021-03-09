package no.mnemonic.messaging.documentchannel.kafka;

/**
 * A transport object representing a document coming from Kafka
 *
 * @param <T> document type
 */
public class KafkaDocument<T> {

  private final T document;
  private final String cursor;

  public KafkaDocument(T document, String cursor) {
    this.document = document;
    this.cursor = cursor;
  }

  public T getDocument() {
    return document;
  }

  public String getCursor() {
    return cursor;
  }
}
