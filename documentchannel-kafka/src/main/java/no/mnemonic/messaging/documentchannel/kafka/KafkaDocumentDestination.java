package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.messaging.documentchannel.DocumentChannel;
import no.mnemonic.messaging.documentchannel.DocumentDestination;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.atomic.AtomicReference;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;

/**
 * Kafka version of a document channel destination, which writes the document to a configured
 * Kafka topic. The channel supports String or byte array documents.
 *
 * @param <T> document type
 */
public class KafkaDocumentDestination<T> implements DocumentDestination<T> {

  private final KafkaProducerProvider provider;
  private final Class<T> type;
  private final String topicName;
  private final boolean flushAfterWrite;
  private final boolean disabled;

  private final AtomicReference<KafkaProducer<String, T>> currentProducer = new AtomicReference<>();

  private KafkaDocumentDestination(
          KafkaProducerProvider provider,
          Class<T> type,
          String topicName,
          boolean flushAfterWrite, boolean disabled) {
    if (provider == null) throw new IllegalArgumentException("provider not set");
    if (type == null) throw new IllegalArgumentException("type not set");
    if (topicName == null) throw new IllegalArgumentException("topicName not set");
    if (!provider.hasType(type)) throw new IllegalArgumentException("Provider does not support type, maybe add a serializer for " + type);
    this.disabled = disabled;
    this.provider = provider;
    this.type = type;
    this.topicName = topicName;
    this.flushAfterWrite = flushAfterWrite;
  }

  @Override
  public DocumentChannel<T> getDocumentChannel() {
    if (disabled) return new NullChannel<>();
    return new DocumentChannel<T>() {
      @Override
      public void sendDocument(T doc) {
        writeDocument(doc);
      }

      @Override
      public <K> void sendDocument(T document, K documentKey, DocumentCallback<K> callback) {
        writeDocument(document, documentKey, callback);
      }

      @Override
      public void flush() {
        getProducer().flush();
      }
    };
  }

  @Override
  public void close() {
    currentProducer.updateAndGet(p -> {
      ifNotNullDo(p, KafkaProducer::close);
      return null;
    });
  }


  //private methods

  private void writeDocument(T doc) {
    getProducer().send(new ProducerRecord<>(topicName, doc));
    if (flushAfterWrite) getProducer().flush();
  }

  private <K> void writeDocument(T doc, K documentKey, DocumentChannel.DocumentCallback<K> callback) {
    getProducer().send(new ProducerRecord<>(topicName, doc), (metadata, exception) -> {
      if (metadata != null) callback.documentAccepted(documentKey);
      else if (exception != null) callback.channelError(documentKey, exception);
    });
    if (flushAfterWrite) getProducer().flush();
  }

  private KafkaProducer<String, T> getProducer() {
    return currentProducer.updateAndGet(p -> ifNull(p, () -> provider.createProducer(type)));
  }


  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {

    //fields
    private KafkaProducerProvider kafkaProducerProvider;
    private Class<T> type;
    private String topicName;
    private boolean flushAfterWrite;
    private boolean disabled;

    public KafkaDocumentDestination<T> build() {
      return new KafkaDocumentDestination<>(kafkaProducerProvider, type, topicName, flushAfterWrite, disabled);
    }

    //setters


    public Builder<T> setFlushAfterWrite(boolean flushAfterWrite) {
      this.flushAfterWrite = flushAfterWrite;
      return this;
    }

    public Builder<T> setProducerProvider(KafkaProducerProvider kafkaProducerProvider) {
      this.kafkaProducerProvider = kafkaProducerProvider;
      return this;
    }

    public Builder<T> setType(Class<T> type) {
      this.type = type;
      return this;
    }

    public Builder<T> setTopicName(String topicName) {
      this.topicName = topicName;
      return this;
    }

    public Builder<T> setDisabled(boolean disabled) {
      this.disabled = disabled;
      return this;
    }
  }


}
