package no.mnemonic.messaging.documentchannel.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * A provider which provides a kafka producer for a configured kafka cluster
 */
public class KafkaProducerProvider {

  private static final int DEFAULT_RETRIES = 5;
  private static final int DEFAULT_BATCH_SIZE = 2000;
  private static final int DEFAULT_MAX_BATCH_WAIT = 1000;
  private static final int DEFAULT_MAX_REQUEST_SIZE = 1048576; // 1MB
  private static final int DEFAULT_TIMEOUT_MILLIS = 30000;
  private static final int DEFAULT_MAX_BLOCK_MILLIS = 10_000;
  private static final int DEFAULT_SEND_BUFFER_SIZE = 131072;

  private final String kafkaHosts;
  private final int kafkaPort;

  private KafkaProducerProvider(String kafkaHosts, int kafkaPort) {
    this.kafkaHosts = kafkaHosts;
    this.kafkaPort = kafkaPort;
  }

  public <T> KafkaProducer<String, T> createProducer(Class<T> type) {
    return new KafkaProducer<>(
            createProperties(),
            new StringSerializer(),  // Key serializer
            createSerializer(type) // Value serializer
    );
  }

  private <T> Serializer<T> createSerializer(Class<T> type) {
    if (type.equals(String.class)) {
      return (Serializer<T>) new StringSerializer();
    } else if (type.equals(byte[].class)) {
      return (Serializer<T>) new ByteArraySerializer();
    } else {
      throw new IllegalArgumentException("Invalid type: " + type);
    }
  }

  private Map<String, Object> createProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(BOOTSTRAP_SERVERS_CONFIG, createBootstrapServerList()); // expect List<String>
    properties.put(ACKS_CONFIG, "1");
    properties.put(COMPRESSION_TYPE_CONFIG, "none");
    properties.put(RETRIES_CONFIG, DEFAULT_RETRIES);
    properties.put(BATCH_SIZE_CONFIG, DEFAULT_BATCH_SIZE);
    properties.put(LINGER_MS_CONFIG, DEFAULT_MAX_BATCH_WAIT);
    properties.put(CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    properties.put(MAX_REQUEST_SIZE_CONFIG, DEFAULT_MAX_REQUEST_SIZE);
    properties.put(REQUEST_TIMEOUT_MS_CONFIG, DEFAULT_TIMEOUT_MILLIS);
    properties.put(MAX_BLOCK_MS_CONFIG, DEFAULT_MAX_BLOCK_MILLIS);
    properties.put(SEND_BUFFER_CONFIG, DEFAULT_SEND_BUFFER_SIZE);
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

    public KafkaProducerProvider build() {
      return new KafkaProducerProvider(kafkaHosts, kafkaPort);
    }

    public Builder setKafkaHosts(String kafkaHosts) {
      this.kafkaHosts = kafkaHosts;
      return this;
    }

    public Builder setKafkaPort(int kafkaPort) {
      this.kafkaPort = kafkaPort;
      return this;
    }
  }

}
