package no.mnemonic.messaging.documentchannel.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * A provider which provides a kafka consumer for a kafka cluster and groupID.
 */
public class KafkaConsumerProvider {

  private static final int DEFAULT_REQUEST_TIMEOUT_MILLIS = 30000;
  private static final int DEFAULT_SESSION_TIMEOUT_MILLIS = 15000; // Need > consumer poll timeout, to avoid exception of time between subsequent calls to poll() was longer than the configured session.timeout.ms
  private static final int DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 3000;
  private static final int DEFAULT_MAX_POLL_RECORDS = 500;

  public enum OffsetResetStrategy {
    latest, earliest, none;
  }

  private final String kafkaHosts;
  private final int kafkaPort;
  private final String groupID;
  private final OffsetResetStrategy offsetResetStrategy;
  private final boolean autoCommit;
  private final int heartbeatIntervalMs;
  private final int requestTimeoutMs;
  private final int sessionTimeoutMs;
  private final int maxPollRecords;

  private KafkaConsumerProvider(
          String kafkaHosts,
          int kafkaPort,
          String groupID,
          OffsetResetStrategy offsetResetStrategy,
          boolean autoCommit,
          int heartbeatIntervalMs,
          int requestTimeoutMs,
          int sessionTimeoutMs,
          int maxPollRecords
  ) {
    this.kafkaHosts = kafkaHosts;
    this.kafkaPort = kafkaPort;
    this.groupID = groupID;
    this.offsetResetStrategy = offsetResetStrategy;
    this.autoCommit = autoCommit;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.requestTimeoutMs = requestTimeoutMs;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.maxPollRecords = maxPollRecords;
  }

  public <T> KafkaConsumer<String, T> createConsumer(Class<T> type) {
    return new KafkaConsumer<>(
            createProperties(),
            new StringDeserializer(),
            createDeserializer(type)
    );
  }

  private <T> Deserializer<T> createDeserializer(Class<T> type) {
    if (type.equals(String.class)) {
      return (Deserializer<T>) new StringDeserializer();
    } else if (type.equals(byte[].class)) {
      return (Deserializer<T>) new ByteArrayDeserializer();
    } else {
      throw new IllegalArgumentException("Invalid type: " + type);
    }
  }

  private Map<String, Object> createProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(BOOTSTRAP_SERVERS_CONFIG, createBootstrapServerList()); // expect List<String>
    properties.put(GROUP_ID_CONFIG, groupID);
    properties.put(AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy.name());
    properties.put(ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
    properties.put(CLIENT_ID_CONFIG, UUID.randomUUID().toString());

    properties.put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
    properties.put(SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
    properties.put(MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    properties.put(HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
    return properties;
  }

  private List<String> createBootstrapServerList() {
    return Arrays.stream(kafkaHosts.split(","))
            .map(h -> String.format("%s:%d", h, kafkaPort))
            .collect(Collectors.toList());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String kafkaHosts;
    private int kafkaPort;
    private String groupID;

    private OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.earliest;
    private boolean autoCommit = false;
    private int heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MILLIS;
    private int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MILLIS;
    private int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MILLIS;
    private int maxPollRecords = DEFAULT_MAX_POLL_RECORDS;


    public KafkaConsumerProvider build() {
      return new KafkaConsumerProvider(kafkaHosts, kafkaPort, groupID, offsetResetStrategy, autoCommit, heartbeatIntervalMs, requestTimeoutMs, sessionTimeoutMs, maxPollRecords);
    }

    public Builder setKafkaHosts(String kafkaHosts) {
      this.kafkaHosts = kafkaHosts;
      return this;
    }

    public Builder setKafkaPort(int kafkaPort) {
      this.kafkaPort = kafkaPort;
      return this;
    }

    public Builder setGroupID(String groupID) {
      this.groupID = groupID;
      return this;
    }

    public Builder setOffsetResetStrategy(OffsetResetStrategy offsetResetStrategy) {
      this.offsetResetStrategy = offsetResetStrategy;
      return this;
    }

    public Builder setAutoCommit(boolean autoCommit) {
      this.autoCommit = autoCommit;
      return this;
    }

    public Builder setHeartbeatIntervalMs(int heartbeatIntervalMs) {
      this.heartbeatIntervalMs = heartbeatIntervalMs;
      return this;
    }

    public Builder setRequestTimeoutMs(int requestTimeoutMs) {
      this.requestTimeoutMs = requestTimeoutMs;
      return this;
    }

    public Builder setSessionTimeoutMs(int sessionTimeoutMs) {
      this.sessionTimeoutMs = sessionTimeoutMs;
      return this;
    }

    public Builder setMaxPollRecords(int maxPollRecords) {
      this.maxPollRecords = maxPollRecords;
      return this;
    }
  }

}
