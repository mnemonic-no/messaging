package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.messaging.documentchannel.DocumentBatch;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import no.mnemonic.messaging.documentchannel.DocumentChannelSubscription;
import no.mnemonic.messaging.documentchannel.DocumentSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.commons.utilities.collections.ListUtils.addToList;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

/**
 * Kafka version of a document channel source. The source is configured with a kafka cluster, topic and groupID.
 * Multiple sources may be configured to the same topic and groupID, which will load-balance the incoming documents
 * between the active sources.
 * <p>
 * Multiple sources with different groupIDs will receive individual copies of each document on the topic.
 *
 * @param <T> document type
 */
public class KafkaDocumentSource<T> implements DocumentSource<T> {

  private static final int CONSUMER_POLL_TIMEOUT_MILLIS = 1000;
  private static final Logger LOGGER = Logging.getLogger(KafkaDocumentSource.class);

  private final KafkaConsumerProvider provider;
  private final Class<T> type;
  private final List<String> topicName;
  private final boolean commitSync;

  private final AtomicBoolean subscriberAttached = new AtomicBoolean();
  private final AtomicReference<KafkaConsumer<String, T>> currentConsumer = new AtomicReference<>();
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  private KafkaDocumentSource(
          KafkaConsumerProvider provider,
          Class<T> type,
          List<String> topicName,
          boolean commitSync) {
    this.commitSync = commitSync;
    if (provider == null) throw new IllegalArgumentException("provider not set");
    if (type == null) throw new IllegalArgumentException("type not set");
    if (CollectionUtils.isEmpty(topicName)) throw new IllegalArgumentException("topicName not set");
    this.provider = provider;
    this.type = type;
    this.topicName = topicName;
  }

  @Override
  public DocumentChannelSubscription createDocumentSubscription(DocumentChannelListener<T> listener) {
    if (listener == null) throw new IllegalArgumentException("listener not set");
    if (currentConsumer.get() != null) throw new IllegalStateException("Subscriber already created");
    KafkaConsumer<String, T> consumer = getConsumer();
    executorService.submit(new ConsumerWorker(consumer, listener));
    return this::close;
  }

  @Override
  public DocumentBatch<T> poll(long duration, TimeUnit timeUnit) {
    if (timeUnit == null) throw new IllegalArgumentException("timeUnit not set");
    if (subscriberAttached.get()) throw new IllegalStateException("This channel already has a subscriber");
    KafkaConsumer<String, T> consumer = getConsumer();
    ConsumerRecords<String, T> records = consumer.poll(timeUnit.toMillis(duration));
    return createBatch(records);
  }

  @Override
  public void close() {
    executorService.shutdown();
    tryTo(() -> executorService.awaitTermination(10, TimeUnit.SECONDS));
    currentConsumer.updateAndGet(c -> {
      ifNotNullDo(c, KafkaConsumer::close);
      return null;
    });
  }


  //private methods


  private DocumentBatch<T> createBatch(ConsumerRecords<String, T> records) {
    List<T> result = ListUtils.list();
    if (records != null) {
      for (ConsumerRecord<String, T> record : records) {
        result.add(record.value());
      }
    }
    return new DocumentBatch<T>() {
      @Override
      public Collection<T> getDocuments() {
        return result;
      }

      @Override
      public void acknowledge() {
        if (commitSync) {
          getConsumer().commitSync();
        } else {
          getConsumer().commitAsync();
        }
      }
    };
  }

  private KafkaConsumer<String, T> getConsumer() {
    return currentConsumer.updateAndGet(p -> ifNull(p, () -> {
      KafkaConsumer<String, T> consumer = provider.createConsumer(type);
      consumer.subscribe(topicName);
      return consumer;
    }));
  }

  private class ConsumerWorker implements Runnable {
    private final KafkaConsumer<String, T> consumer;
    private final DocumentChannelListener<T> listener;

    ConsumerWorker(KafkaConsumer<String, T> consumer, DocumentChannelListener<T> listener) {
      this.consumer = consumer;
      this.listener = listener;
    }

    @Override
    public void run() {
      try {
        while (!executorService.isShutdown()) {
         consumeBatch();
        }
      } catch (Exception e){
        LOGGER.error(e, "Error in ConsumeWorker");
      } finally {
        consumer.close();
      }
    }

    private void consumeBatch() {
      ConsumerRecords<String, T> records = consumer.poll(CONSUMER_POLL_TIMEOUT_MILLIS);
      for (ConsumerRecord<String, T> record : records) {
        listener.documentReceived(record.value());
      }
      //when all documents in batch is consumed, send commit to consumer
      if (commitSync) {
        consumer.commitSync();
      } else {
        consumer.commitAsync();
      }
    }
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {

    //fields
    private KafkaConsumerProvider kafkaConsumerProvider;
    private Class<T> type;
    private List<String> topicName;
    private boolean commitSync;

    public KafkaDocumentSource<T> build() {
      return new KafkaDocumentSource<>(kafkaConsumerProvider, type, topicName, commitSync);
    }

    //setters


    public Builder<T> setCommitSync(boolean commitSync) {
      this.commitSync = commitSync;
      return this;
    }

    public Builder<T> setConsumerProvider(KafkaConsumerProvider kafkaConsumerProvider) {
      this.kafkaConsumerProvider = kafkaConsumerProvider;
      return this;
    }

    public Builder<T> setType(Class<T> type) {
      this.type = type;
      return this;
    }

    public Builder<T> setTopicName(String... topicName) {
      this.topicName = list(topicName);
      return this;
    }

    public Builder<T> setTopicName(List<String> topicName) {
      this.topicName = topicName;
      return this;
    }

    public Builder<T> addTopicName(String topicName) {
      this.topicName = addToList(this.topicName, topicName);
      return this;
    }
  }


}