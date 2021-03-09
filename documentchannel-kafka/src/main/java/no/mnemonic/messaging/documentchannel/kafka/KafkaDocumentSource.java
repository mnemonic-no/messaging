package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.messaging.documentchannel.DocumentBatch;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import no.mnemonic.messaging.documentchannel.DocumentChannelSubscription;
import no.mnemonic.messaging.documentchannel.DocumentSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.collections.ListUtils.addToList;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.collections.MapUtils.map;
import static no.mnemonic.commons.utilities.collections.SetUtils.addToSet;
import static no.mnemonic.commons.utilities.collections.SetUtils.set;
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
public class KafkaDocumentSource<T> implements DocumentSource<T>, MetricAspect {

  public enum CommitType {
    sync, async, none;
  }

  private final KafkaConsumerProvider provider;
  private final Class<T> type;
  private final List<String> topicName;
  private final CommitType commitType;

  private final AtomicBoolean subscriberAttached = new AtomicBoolean();
  private final AtomicReference<KafkaConsumer<String, T>> currentConsumer = new AtomicReference<>();
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final Set<Consumer<Exception>> errorListeners;

  private final AtomicBoolean consumerRunning = new AtomicBoolean();
  private final LongAdder consumerRetryProcessError = new LongAdder();
  private final LongAdder consumerRetryKafkaError = new LongAdder();
  private final KafkaCursor currentCursor = new KafkaCursor();

  private KafkaDocumentSource(
          KafkaConsumerProvider provider,
          Class<T> type,
          List<String> topicName,
          CommitType commitType,
          Set<Consumer<Exception>> errorListeners
  ) {
    this.commitType = commitType;
    this.errorListeners = errorListeners;
    if (provider == null) throw new IllegalArgumentException("provider not set");
    if (type == null) throw new IllegalArgumentException("type not set");
    if (CollectionUtils.isEmpty(topicName)) throw new IllegalArgumentException("topicName not set");
    if (!provider.hasType(type)) throw new IllegalArgumentException("Provider does not support type, maybe add a deserializer for " + type);
    this.provider = provider;
    this.type = type;
    this.topicName = topicName;
  }

  /**
   * Seek to cursor
   *
   * @param cursor A string representation of a cursor as returned in a {@link KafkaDocument}. A null cursor will not change the offset of the consumer.
   * @throws KafkaInvalidSeekException if the cursor is invalid, or points to a timestamp which is not reachable
   */
  public void seek(String cursor) throws KafkaInvalidSeekException {
    if (cursor == null) {
      return;
    }
    //parse cursor into current cursor
    currentCursor.parse(cursor);
    //for each active topic, seek each partition to the position specified by the cursor
    for (String topic : topicName) {
      seek(topic, currentCursor.getPointers().get(topic));
    }
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    return new MetricsData()
            .addData("kafka.consumer.alive", consumerRunning.get() ? 1 : 0)
            .addData("kafka.consumer.processing.error.retry", consumerRetryProcessError.longValue())
            .addData("kafka.consumer.kafka.error.retry", consumerRetryKafkaError.longValue());
  }

  public KafkaCursor getCursor() {
    return currentCursor;
  }

  @Override
  public DocumentChannelSubscription createDocumentSubscription(DocumentChannelListener<T> listener) {
    if (listener == null) throw new IllegalArgumentException("listener not set");
    if (subscriberAttached.get()) throw new IllegalStateException("Subscriber already created");
    executorService.submit(new KafkaConsumerWorker<>(getCurrentConsumerOrSubscribe(), listener, commitType, topicName, createCallbackInterface()));

    return this::close;
  }

  private ConsumerCallbackInterface createCallbackInterface() {
    return new ConsumerCallbackInterface() {
      @Override
      public void consumerRunning(boolean running) {
        consumerRunning.set(running);
      }

      @Override
      public void retryError(Exception e) {
        consumerRetryKafkaError.increment();
        errorListeners.forEach(l -> l.accept(e));
      }

      @Override
      public void register(ConsumerRecord<?, ?> record) {
        currentCursor.register(record);
      }

      @Override
      public void processError(Exception e) {
        consumerRetryProcessError.increment();
        errorListeners.forEach(l -> l.accept(e));
      }

      @Override
      public boolean isShutdown() {
        return executorService.isShutdown();
      }

      @Override
      public String cursor() {
        return currentCursor.toString();
      }
    };
  }

  @Override
  public DocumentBatch<T> poll(Duration duration) {
    return createBatch(pollConsumerRecords(duration));
  }

  public DocumentBatch<KafkaDocument<T>> pollDocuments(Duration duration) {
    return createDocuments(pollConsumerRecords(duration));
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

  ConsumerRecords<String, T> pollConsumerRecords(Duration duration) {
    if (duration == null) throw new IllegalArgumentException("duration not set");
    if (subscriberAttached.get()) throw new IllegalStateException("This channel already has a subscriber");
    //the semantics of the document source does not require an explicit "subscribe()"-operation
    return getCurrentConsumerOrSubscribe().poll(duration);
  }

  private void seek(String topic, Map<Integer, KafkaCursor.OffsetAndTimestamp> cursorPointers) throws KafkaInvalidSeekException {
    if (currentConsumer.get() != null) {
      throw new IllegalStateException("Cannot seek for consumer which is already set");
    }
    currentConsumer.set(provider.createConsumer(type));

    //determine active partitions for this topic
    Map<Integer, PartitionInfo> partitionInfos = map(getConsumerOrFail().partitionsFor(topic), p -> MapUtils.pair(p.partition(), p));
    //assign all partitions to this consumer
    getConsumerOrFail().assign(SetUtils.set(partitionInfos.values(), p -> new TopicPartition(topic, p.partition())));
    //find timestamp to seek to for each partition in the cursor (all other partitions will stay at initial offset set by OffsetResetStrategy)
    Map<TopicPartition, Long> partitionsToSeek = new HashMap<>();
    map(cursorPointers).forEach((partition, offsetAndTimestamp) -> partitionsToSeek.put(
            new TopicPartition(topic, partition),
            offsetAndTimestamp.getTimestamp()
    ));

    //lookup offsets by timestamp from Kafka
    Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap = getConsumerOrFail().offsetsForTimes(partitionsToSeek);

    //for each partition, seek to requested position, or fail
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entries : partitionOffsetMap.entrySet()) {
      if (entries.getValue() == null)
        throw new KafkaInvalidSeekException("Unable to skip to requested position for partition " + entries.getKey().partition());
      getConsumerOrFail().seek(entries.getKey(), entries.getValue().offset());
    }

  }

  private DocumentBatch<KafkaDocument<T>> createDocuments(ConsumerRecords<String, T> records) {
    List<KafkaDocument<T>> result = ListUtils.list();
    if (records != null) {
      for (ConsumerRecord<String, T> record : records) {
        currentCursor.register(record);
        result.add(new KafkaDocument<>(record.value(), currentCursor.toString()));
      }
    }
    return new KafkaDocumentBatch<>(result, commitType, getConsumerOrFail());
  }

  private DocumentBatch<T> createBatch(ConsumerRecords<String, T> records) {
    List<T> result = ListUtils.list();
    if (records != null) {
      for (ConsumerRecord<String, T> record : records) {
        currentCursor.register(record);
        result.add(record.value());
      }
    }
    return new KafkaDocumentBatch<>(result, commitType, getConsumerOrFail());
  }

  private static class KafkaDocumentBatch<D> implements DocumentBatch<D> {
    private final Collection<D> documents;
    private final CommitType commitType;
    private final KafkaConsumer<?, ?> consumer;

    private KafkaDocumentBatch(Collection<D> documents, CommitType commitType, KafkaConsumer<?, ?> consumer) {
      this.documents = documents;
      this.commitType = commitType;
      this.consumer = consumer;
    }

    @Override
    public Collection<D> getDocuments() {
      return documents;
    }

    @Override
    public void acknowledge() {
      if (commitType == CommitType.sync) {
        consumer.commitSync();
      } else if (commitType == CommitType.async) {
        consumer.commitAsync();
      }
    }
  }

  private KafkaConsumer<String, T> getConsumerOrFail() {
    if (currentConsumer.get() == null) throw new IllegalStateException("Consumer not set");
    return currentConsumer.get();
  }

  private KafkaConsumer<String, T> getCurrentConsumerOrSubscribe() {
    KafkaConsumer<String, T> consumer = currentConsumer.get();
    if (consumer == null) {
      consumer = provider.createConsumer(type);
      consumer.subscribe(topicName);
      currentConsumer.set(consumer);
    }
    return consumer;
  }

  interface ConsumerCallbackInterface {
    void consumerRunning(boolean running);

    void retryError(Exception e);

    void register(ConsumerRecord<?, ?> record);

    void processError(Exception e);

    boolean isShutdown();

    String cursor();
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {

    //fields
    private KafkaConsumerProvider kafkaConsumerProvider;
    private Class<T> type;
    private List<String> topicName;
    private CommitType commitType = CommitType.async;
    private Set<Consumer<Exception>> errorListeners = set();

    public KafkaDocumentSource<T> build() {
      return new KafkaDocumentSource<>(kafkaConsumerProvider, type, topicName, commitType, errorListeners);
    }

    //setters

    public Builder<T> setErrorListeners(Set<Consumer<Exception>> errorListeners) {
      this.errorListeners = set(errorListeners);
      return this;
    }

    public Builder addErrorListener(Consumer<Exception> errorListener) {
      this.errorListeners = addToSet(this.errorListeners, errorListener);
      return this;
    }

    public Builder<T> setCommitSync(boolean sync) {
      return setCommitType(sync ? CommitType.sync : CommitType.async);
    }

    public Builder<T> setCommitType(CommitType commitType) {
      this.commitType = commitType;
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
