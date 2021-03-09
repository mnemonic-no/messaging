package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;

class KafkaConsumerWorker<T> implements Runnable {

  private static final int CONSUMER_POLL_TIMEOUT_MILLIS = 1000;
  private static final Logger LOGGER = Logging.getLogger(KafkaConsumerWorker.class);

  private final KafkaConsumer<String, T> consumer;
  private final DocumentChannelListener<T> listener;
  private final KafkaDocumentSource.ConsumerCallbackInterface consumerInterface;
  private final KafkaDocumentSource.CommitType commitType;
  private final Collection<String> topicName;

  KafkaConsumerWorker(
          KafkaConsumer<String, T> consumer,
          DocumentChannelListener<T> listener,
          KafkaDocumentSource.CommitType commitType,
          Collection<String> topicName,
          KafkaDocumentSource.ConsumerCallbackInterface consumerInterface) {
    this.consumer = consumer;
    this.listener = listener;
    this.consumerInterface = consumerInterface;
    this.commitType = commitType;
    this.topicName = topicName;
  }

  @Override
  public void run() {
    try {
      consumerInterface.consumerRunning(true);
      while (!consumerInterface.isShutdown()) {
        consumeBatchWithResubscribeWhenFail();
      }
    } catch (Exception e) {
      LOGGER.error(e, "Kafka ConsumerWorker failed unrecoverable, stopping ConsumerWorker thread");
    } finally {
      LOGGER.info("Close Kafka Consumer");
      consumerInterface.consumerRunning(false);
      consumer.close();
    }
  }

  private void consumeBatchWithResubscribeWhenFail() {
    try {
      consumeBatch();
    } catch (Exception e) {
      // Error from Kafka clients polling or committing offsets
      LOGGER.error(e, "Kafka ConsumerWorker fail, " +
              "reset consumer offsets and resubscribing to the topics: " + String.join(",", topicName));
      consumerInterface.retryError(e);
      resetOffsets();
      consumer.unsubscribe();
      consumer.subscribe(topicName);
    }
  }

  private void consumeBatch() {
    ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MILLIS));
    boolean consumed = false;
    try {
      for (ConsumerRecord<String, T> record : records) {
        consumerInterface.register(record);
        if (listener instanceof KafkaDocumentChannelListener) {
          ((KafkaDocumentChannelListener<T>) listener).documentReceived(new KafkaDocument<>(record.value(), consumerInterface.cursor()));
        } else {
          listener.documentReceived(record.value());
        }
        consumed = true;
      }
    } catch (Exception e) {
      // Error when processing events
      LOGGER.error(e, "Error when processing Kafka consumed records, reset consumer offsets");
      consumerInterface.processError(e);
      resetOffsets();
      return; // cause next poll with new offsets
    }
    //when all documents in batch is consumed, send commit to consumer
    if (consumed) {
      if (commitType == KafkaDocumentSource.CommitType.sync) {
        consumer.commitSync();
      } else if (commitType == KafkaDocumentSource.CommitType.async) {
        consumer.commitAsync();
      }
    }
  }

  private void resetOffsets() {
    LOGGER.info("Reset offsets for assigned Kafka partitions " + consumer.assignment());
    Set<TopicPartition> assignment = consumer.assignment();
    consumer.committed(assignment)
            .entrySet().stream()
            // no offsets can be fetched from Kafka broker (e.g. no commits has been performed on the partition)
            .filter(e -> e.getValue() != null)
            .forEach(e -> consumer.seek(e.getKey(), e.getValue()));
  }
}
