package no.mnemonic.messaging.documentchannel.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
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

  private static final int REQUEST_TIMEOUT_MS = 30000;
  private static final int SESSION_TIMEOUT_MS = 15000; // Need > consumer poll timeout, to avoid exception of time between subsequent calls to poll() was longer than the configured session.timeout.ms
  private static final int HEARTBEAT_INTERVAL_MS = 3000;
  private static final int MAX_POLL_RECORDS = 500;

  private final String kafkaHosts;
  private final int kafkaPort;
  private final String groupID;

  private KafkaConsumerProvider(String kafkaHosts, int kafkaPort, String groupID) {
    this.kafkaHosts = kafkaHosts;
    this.kafkaPort = kafkaPort;
    this.groupID = groupID;
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
    properties.put(AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
    properties.put(ENABLE_AUTO_COMMIT_CONFIG, true);
    properties.put(CLIENT_ID_CONFIG, UUID.randomUUID().toString());

    properties.put(REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
    properties.put(SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
    properties.put(MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
    properties.put(HEARTBEAT_INTERVAL_MS_CONFIG, HEARTBEAT_INTERVAL_MS);
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

    public KafkaConsumerProvider build() {
      return new KafkaConsumerProvider(kafkaHosts, kafkaPort, groupID);
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
  }

}
