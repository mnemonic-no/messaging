package no.mnemonic.messaging.documentchannel.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import no.mnemonic.commons.junit.docker.DockerTestUtils;
import no.mnemonic.messaging.documentchannel.DocumentChannel;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KafkaDocumentChannelCustomSerializerTest {

  @ClassRule
  public static DockerComposeRule docker = DockerComposeRule.builder()
          .file("src/test/resources/docker-compose-kafka.yml")
          .projectName(ProjectName.fromString(UUID.randomUUID().toString().replace("-", "")))
          .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
          .build();

  private Semaphore semaphore = new Semaphore(0);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Mock
  private DocumentChannelListener<MyObject> listener;
  @Mock
  private DocumentChannel.DocumentCallback<MyObject> callback;
  @Mock
  private Consumer<Exception> errorListener;
  private Collection<AutoCloseable> channels = new ArrayList<>();
  private String topic = UUID.randomUUID().toString();

  @Before
  public void setUp() {
    doAnswer(i -> {
      semaphore.release();
      return null;
    }).when(listener).documentReceived(any());
  }

  @After
  public void tearDown() {
    channels.forEach(c -> tryTo(c::close));
  }

  @Test
  public void testCustomSerializer() throws InterruptedException {
    topic = "singleConsumerGroupTest";
    KafkaDocumentDestination<MyObject> senderChannel = setupDestination();
    KafkaDocumentSource<MyObject> receiverChannel1 = setupSource("group");
    KafkaDocumentSource<MyObject> receiverChannel2 = setupSource("group");
    receiverChannel1.createDocumentSubscription(listener);
    receiverChannel2.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument(new MyObject(1, "obj1"));
    senderChannel.getDocumentChannel().sendDocument(new MyObject(2, "obj2"));
    senderChannel.getDocumentChannel().sendDocument(new MyObject(3, "obj3"));
    assertTrue(semaphore.tryAcquire(3, 10, TimeUnit.SECONDS));
    verify(listener).documentReceived(argThat(o->o.getId()==1));
    verify(listener).documentReceived(argThat(o->o.getId()==2));
    verify(listener).documentReceived(argThat(o->o.getId()==3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void creatingDestinationFailsForUnknownType() {
    KafkaProducerProvider producerProvider = KafkaProducerProvider.builder()
              .setKafkaHosts(kafkaHost())
              .setKafkaPort(kafkaPort())
              .build();
     KafkaDocumentDestination.<MyObject>builder()
            .setProducerProvider(producerProvider)
            .setFlushAfterWrite(false)
            .setTopicName(topic)
            .setType(MyObject.class)
            .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void creatingSourceFailsForUnknownType() {
    KafkaConsumerProvider consumerProvider = KafkaConsumerProvider.builder()
            .setKafkaHosts(kafkaHost())
            .setKafkaPort(kafkaPort())
            .setGroupID("test")
            .build();
    KafkaDocumentSource.<MyObject>builder()
            .setConsumerProvider(consumerProvider)
            .setTopicName(topic)
            .setType(MyObject.class)
            .build();
  }

  //private
  private KafkaDocumentDestination<MyObject> setupDestination() {
    KafkaDocumentDestination<MyObject> channel = KafkaDocumentDestination.<MyObject>builder()
            .setProducerProvider(createProducerProvider())
            .setFlushAfterWrite(false)
            .setTopicName(topic)
            .setKeySerializer(MyObject::getType)
            .setType(MyObject.class)
            .build();
    channels.add(channel);
    return channel;
  }

  private KafkaDocumentSource<MyObject> setupSource(String group) {
    // noinspection unchecked
    KafkaDocumentSource<MyObject> channel = KafkaDocumentSource.<MyObject>builder()
            .setConsumerProvider(createConsumerProvider(group))
            .addErrorListener(errorListener)
            .setTopicName(topic)
            .setType(MyObject.class)
            .setCommitType(KafkaDocumentSource.CommitType.sync)
            .build();
    channels.add(channel);
    return channel;
  }

  private KafkaProducerProvider createProducerProvider() {
    return KafkaProducerProvider.builder()
            .setKafkaHosts(kafkaHost())
            .setKafkaPort(kafkaPort())
            .addSerializer(MyObject.class, new Serializer<MyObject>() {
              @Override
              public void configure(Map<String, ?> configs, boolean isKey) {
              }

              @Override
              public byte[] serialize(String topic, MyObject data) {
                try {
                  return MAPPER.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                  throw new IllegalStateException(e);
                }
              }

              @Override
              public void close() {
              }
            })
            .build();
  }

  private KafkaConsumerProvider createConsumerProvider(String group) {
    return KafkaConsumerProvider.builder()
            .setMaxPollRecords(2)
            .setKafkaHosts(kafkaHost())
            .setKafkaPort(kafkaPort())
            .setAutoCommit(false)
            .setHeartbeatIntervalMs(1000)
            .setRequestTimeoutMs(11000)
            .setSessionTimeoutMs(10000)
            .setMaxPollIntervalMs(2000)
            .setGroupID(group)
            .addDeserializer(MyObject.class, new Deserializer<MyObject>() {
              @Override
              public void configure(Map<String, ?> configs, boolean isKey) {

              }

              @Override
              public MyObject deserialize(String topic, byte[] data) {
                try {
                  return MAPPER.readValue(data, MyObject.class);
                } catch (IOException e) {
                  throw new IllegalStateException(e);
                }
              }

              @Override
              public void close() {

              }
            })
            .build();
  }

  private String kafkaHost() {
    return ifNull(DockerTestUtils.getDockerHost(), "localhost");
  }

  private int kafkaPort() {
    return docker.containers()
            .container("kafka")
            .port(9094)
            .getExternalPort();
  }

  public static class MyObject {
    private int id;
    private String type;

    public MyObject() {
    }

    public MyObject(int id, String type) {
      this.id = id;
      this.type = type;
    }

    public int getId() {
      return id;
    }

    public MyObject setId(int id) {
      this.id = id;
      return this;
    }

    public String getType() {
      return type;
    }

    public MyObject setType(String type) {
      this.type = type;
      return this;
    }
  }

}
