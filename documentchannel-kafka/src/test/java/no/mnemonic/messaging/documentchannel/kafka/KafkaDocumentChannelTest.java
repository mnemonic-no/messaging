package no.mnemonic.messaging.documentchannel.kafka;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNull;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaDocumentChannelTest {

  @ClassRule
  public static DockerComposeRule docker = DockerComposeRule.builder()
          .file("src/test/resources/docker-compose-kafka.yml")
          .projectName(ProjectName.fromString(UUID.randomUUID().toString().replace("-", "")))
          .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
          .build();

  private Semaphore semaphore = new Semaphore(0);

  @Mock
  private DocumentChannelListener<String> listener;
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
  public void singleConsumerGroup() throws InterruptedException {
    KafkaDocumentDestination<String> senderChannel = setupDestination();
    KafkaDocumentSource<String> receiverChannel1 = setupSource("group");
    KafkaDocumentSource<String> receiverChannel2 = setupSource("group");
    receiverChannel1.createDocumentSubscription(listener);
    receiverChannel2.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument("mydoc1");
    senderChannel.getDocumentChannel().sendDocument("mydoc2");
    senderChannel.getDocumentChannel().sendDocument("mydoc3");
    assertTrue(semaphore.tryAcquire(3, 10, TimeUnit.SECONDS));
    verify(listener).documentReceived("mydoc1");
    verify(listener).documentReceived("mydoc2");
    verify(listener).documentReceived("mydoc3");
  }

  @Test
  public void multipleConsumerGroup() throws InterruptedException {
    KafkaDocumentDestination<String> senderChannel = setupDestination();
    KafkaDocumentSource<String> receiverChannel1 = setupSource("group1");
    KafkaDocumentSource<String> receiverChannel2 = setupSource("group2");
    receiverChannel1.createDocumentSubscription(listener);
    receiverChannel2.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument("mydoc1");
    senderChannel.getDocumentChannel().sendDocument("mydoc2");
    senderChannel.getDocumentChannel().sendDocument("mydoc3");
    assertTrue(semaphore.tryAcquire(6, 10, TimeUnit.SECONDS));
    verify(listener, times(2)).documentReceived("mydoc1");
    verify(listener, times(2)).documentReceived("mydoc2");
    verify(listener, times(2)).documentReceived("mydoc3");
  }


  //private
  private KafkaDocumentDestination<String> setupDestination() {
    KafkaDocumentDestination<String> channel = KafkaDocumentDestination.<String>builder()
            .setProducerProvider(createProducerProvider())
            .setTopicName(topic)
            .setType(String.class)
            .build();
    channels.add(channel);
    return channel;
  }

  private KafkaDocumentSource<String> setupSource(String group) {
    KafkaDocumentSource<String> channel = KafkaDocumentSource.<String>builder()
            .setConsumerProvider(createConsumerProvider(group))
            .setTopicName(topic)
            .setType(String.class)
            .build();
    channels.add(channel);
    return channel;
  }

  private KafkaProducerProvider createProducerProvider() {
    return KafkaProducerProvider.builder()
            .setKafkaHosts(kafkaHost())
            .setKafkaPort(kafkaPort())
            .build();
  }

  private KafkaConsumerProvider createConsumerProvider(String group) {
    return KafkaConsumerProvider.builder()
            .setKafkaHosts(kafkaHost())
            .setKafkaPort(kafkaPort())
            .setGroupID(group)
            .build();
  }

  private String kafkaHost() {
    return ifNotNull(System.getenv("DOCKER_HOST"), this::extractHost, "localhost");
  }

  private int kafkaPort() {
    return docker.containers()
            .container("kafka")
            .port(9094)
            .getExternalPort();
  }

  private String extractHost(String dockerHost) {
    Pattern p = Pattern.compile("tcp://(.+):(.+)");
    Matcher m = p.matcher(dockerHost);
    if (!m.matches()) throw new IllegalArgumentException("Illegal docker host: " + dockerHost);
    return m.group(1);
  }

}
