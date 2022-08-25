package no.mnemonic.messaging.documentchannel.kafka;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import no.mnemonic.commons.junit.docker.DockerTestUtils;
import no.mnemonic.messaging.documentchannel.DocumentBatch;
import no.mnemonic.messaging.documentchannel.DocumentChannel;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.collections.SetUtils.set;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KafkaDocumentChannelTest {

  private final Semaphore semaphore = new Semaphore(0);

  @Mock
  private DocumentChannelListener<String> listener;
  @Mock
  private DocumentChannel.DocumentCallback<String> callback;

  @Mock
  Consumer<Exception> errorListener;

  Collection<AutoCloseable> channels = new ArrayList<>();
  private String topic = UUID.randomUUID().toString();

  @ClassRule
  public static DockerComposeRule docker = DockerComposeRule.builder()
          .file("src/test/resources/docker-compose-kafka.yml")
          .projectName(ProjectName.fromString(UUID.randomUUID().toString().replace("-", "")))
          .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
          .build();

  @After
  public void tearDown() {
    channels.forEach(c -> tryTo(c::close));
  }

  @Before
  public void setUp() {
    doAnswer(asVoid(str->semaphore.release())).when(listener).documentReceived(any());
  }

  @Test
  public void initialCursor() throws KafkaInvalidSeekException, InterruptedException {
    useTopic("initialCursor");
    KafkaDocumentDestination<String> sender = setupDestination();
    DocumentChannel<String> senderChannel = sender.getDocumentChannel();
    KafkaDocumentSource<String> receiverChannel1 = setupSource("group1", null, b -> b.setMaxPollRecords(10));

    senderChannel.sendDocument("mydoc1");
    senderChannel.flush();
    Thread.sleep(1); //sleep between messages to ensure they have different timestamps in kafka
    receiverChannel1.createDocumentSubscription(listener);
    KafkaCursor cursor = receiverChannel1.getKafkaCursor();

    senderChannel.sendDocument("mydoc2");
    senderChannel.sendDocument("mydoc3");
    senderChannel.sendDocument("mydoc4");
    senderChannel.flush();

    KafkaDocumentSource<String> receiverChannel2 = setupSource("group2", null, b -> b.setMaxPollRecords(10));
    //make sure the initial pointer points back to BEFORE the first record
    receiverChannel2.seek(cursor.toString());
    DocumentBatch<String> batch = receiverChannel2.poll(Duration.ofSeconds(1));
    assertEquals(list("mydoc2", "mydoc3", "mydoc4"), list(batch.getDocuments()));
  }

  @Test
  public void pollWithAcknowledge() {
    useTopic("pollWithAcknowledgeTest");
    KafkaDocumentDestination<String> senderChannel = setupDestination();

    KafkaDocumentSource<String> receiverChannel1 = setupSource("group", null, b -> b.setMaxPollRecords(2));

    senderChannel.getDocumentChannel().sendDocument("mydoc1");
    senderChannel.getDocumentChannel().sendDocument("mydoc2");
    senderChannel.getDocumentChannel().sendDocument("mydoc3");

    DocumentBatch<String> batch = receiverChannel1.poll(Duration.ofSeconds(10));
    assertEquals(list("mydoc1", "mydoc2"), list(batch.getDocuments()));
    batch.acknowledge();

    batch = receiverChannel1.poll(Duration.ofSeconds(10));
    assertEquals(list("mydoc3"), list(batch.getDocuments()));
    batch.acknowledge();
  }

  @Test
  public void writeWithCallback() {
    useTopic("writeWithCallbackTest");
    KafkaDocumentDestination<String> senderChannel = setupDestination();
    KafkaDocumentSource<String> receiverChannel1 = setupSource("group");

    senderChannel.getDocumentChannel().sendDocument("mydoc1", "doc1", callback);
    senderChannel.getDocumentChannel().sendDocument("mydoc2", "doc2", callback);
    senderChannel.getDocumentChannel().sendDocument("mydoc3", "doc3", callback);
    senderChannel.getDocumentChannel().flush();

    receiverChannel1.poll(Duration.ofSeconds(10));
    verify(callback).documentAccepted("doc1");
    verify(callback).documentAccepted("doc2");
    verify(callback).documentAccepted("doc3");
  }

  @Test
  public void rangeIterate() throws KafkaInvalidSeekException, InterruptedException {
    useTopic("rangeIterateTest");
    KafkaDocumentDestination<String> senderChannel = setupDestination();
    KafkaDocumentSource<String> receiverChannel1 = setupSource("group", null, b -> b.setMaxPollRecords(10));

    senderChannel.getDocumentChannel().sendDocument("mydoc1");
    Thread.sleep(1); //sleep between messages to ensure they have different timestamps in kafka
    senderChannel.getDocumentChannel().sendDocument("mydoc2");
    Thread.sleep(1);
    senderChannel.getDocumentChannel().sendDocument("mydoc3");
    Thread.sleep(1);
    senderChannel.getDocumentChannel().flush();

    String cursor;
    {
      Collection<KafkaDocument<String>> result = receiverChannel1.poll(Duration.ofSeconds(10)).getKafkaDocuments();
      assertEquals(3, result.size());
      Iterator<KafkaDocument<String>> iterator = result.iterator();
      KafkaDocument<String> doc1 = iterator.next();
      KafkaDocument<String> doc2 = iterator.next();
      KafkaDocument<String> doc3 = iterator.next();
      assertEquals("mydoc1", doc1.getDocument());
      assertEquals("mydoc2", doc2.getDocument());
      assertEquals("mydoc3", doc3.getDocument());
      assertNotNull(doc2.getCursor());
      cursor = doc2.getCursor();
    }

    {
      KafkaDocumentSource<String> rangeReceiver = setupSource("range");
      rangeReceiver.seek(cursor);

      Collection<KafkaDocument<String>> result2 = rangeReceiver.poll(Duration.ofSeconds(10)).getKafkaDocuments();
      assertEquals(2, result2.size());
      Iterator<KafkaDocument<String>> iterator2 = result2.iterator();
      KafkaDocument<String> rdoc1 = iterator2.next();
      KafkaDocument<String> rdoc2 = iterator2.next();
      assertEquals("mydoc2", rdoc1.getDocument());
      assertEquals("mydoc3", rdoc2.getDocument());
    }
  }

  @Test
  public void noneCommitTypeWithSubscriber() throws InterruptedException {
    useTopic("noneCommitTypeWithSubscriberTest");
    KafkaDocumentDestination<String> senderChannel = setupDestination();

    for (int i = 0; i < 100; i++) {
      senderChannel.getDocumentChannel().sendDocument("mydoc" + i);
      Thread.sleep(1);
    }
    senderChannel.getDocumentChannel().flush();

    {
      KafkaDocumentSource<String> receiverChannel1 = setupSource("group", s -> s.setCommitType(KafkaDocumentSource.CommitType.none), b -> b.setMaxPollRecords(500));
      receiverChannel1.createDocumentSubscription(listener);
      assertTrue(semaphore.tryAcquire(100, 10, TimeUnit.SECONDS));
      receiverChannel1.close();
    }

    {
      KafkaDocumentSource<String> receiverChannel2 = setupSource("group", s -> s.setCommitType(KafkaDocumentSource.CommitType.none), b -> b.setMaxPollRecords(500));
      receiverChannel2.createDocumentSubscription(listener);
      assertTrue(semaphore.tryAcquire(100, 10, TimeUnit.SECONDS));
      receiverChannel2.close();
    }
  }

  @Test
  public void pollWithoutAcknowledgeResends() {
    useTopic("pollWithoutAcknowledgeResendsTest");
    KafkaDocumentDestination<String> senderChannel = setupDestination();

    KafkaDocumentSource<String> receiverChannel1 = setupSource("group", null, b -> b.setMaxPollRecords(2));

    senderChannel.getDocumentChannel().sendDocument("mydoc1");
    senderChannel.getDocumentChannel().sendDocument("mydoc2");
    senderChannel.getDocumentChannel().sendDocument("mydoc3");

    DocumentBatch<String> batch = receiverChannel1.poll(Duration.ofSeconds(10));
    assertEquals(list("mydoc1", "mydoc2"), list(batch.getDocuments()));
    receiverChannel1.close();

    KafkaDocumentSource<String> receiverChannel2 = setupSource("group", null, b -> b.setMaxPollRecords(2));
    batch = receiverChannel2.poll(Duration.ofSeconds(10));
    assertEquals(list("mydoc1", "mydoc2"), list(batch.getDocuments()));

  }

  @Test
  public void singleConsumerGroup() throws InterruptedException {
    useTopic("singleConsumerGroupTest");
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
  public void retryOnError() throws InterruptedException {
    useTopic("retryOnErrorTest");
    doAnswer(asVoid(str->semaphore.release()))
            .doThrow(new RuntimeException()) //ONE error when first listener is invoked
            .doAnswer(asVoid(str->semaphore.release()))
            .when(listener).documentReceived(any());

    KafkaDocumentDestination<String> senderChannel = setupDestination();
    KafkaDocumentSource<String> receiverChannel = setupSource("group");
    receiverChannel.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument("mydoc1");
    // make sure the first message has been received, so that there is offset committed
    assertTrue(semaphore.tryAcquire(1, 10, TimeUnit.SECONDS));

    senderChannel.getDocumentChannel().sendDocument("mydoc2");
    // check second message has been received
    assertTrue(semaphore.tryAcquire(1, 10, TimeUnit.SECONDS));

    verify(listener, times(1)).documentReceived("mydoc1");
    verify(listener, times(2)).documentReceived("mydoc2");
    verify(errorListener).accept(isA(RuntimeException.class));
  }

  @Test
  public void retryOnKafkaRebalanced() throws InterruptedException {
    useTopic("retryOnKafkaRebalancedTest");
    doAnswer(asVoid(str->semaphore.release()))
            // Demonstrate processing took longer than max.poll.interval.ms that would trigger rebalance of kafka consumer
            .doAnswer(asVoid(str->tryTo(()->TimeUnit.SECONDS.sleep(3L))))
            .doAnswer(asVoid(str->semaphore.release()))
            .when(listener).documentReceived(any());


    KafkaDocumentDestination<String> senderChannel = setupDestination();
    KafkaDocumentSource<String> receiverChannel = setupSource("group");
    receiverChannel.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument("mydoc1");
    // make sure the first message has been received, so that there is offset committed
    assertTrue(semaphore.tryAcquire(1, 20, TimeUnit.SECONDS));

    senderChannel.getDocumentChannel().sendDocument("mydoc2");
    // check second message has been received
    assertTrue(semaphore.tryAcquire(1, 20, TimeUnit.SECONDS));

    verify(listener, times(1)).documentReceived("mydoc1");
    verify(listener, times(2)).documentReceived("mydoc2");
    verify(errorListener).accept(isA(CommitFailedException.class));
  }

  @Test
  public void retryWithMultipleConsumers() throws InterruptedException {
    useTopic("retryWithMultipleConsumersTest");
    Set<String> documents = set();

    doAnswer(asVoid(str->{documents.add(str); semaphore.release(); }))
            .doThrow(new RuntimeException()) //ONE error when first listener is invoked
            .doAnswer(asVoid(str->{documents.add(str); semaphore.release();}))
            .when(listener).documentReceived(any());

    KafkaDocumentDestination<String> senderChannel = setupDestination();
    KafkaDocumentSource<String> receiverChannel1 = setupSource("retryWithMultipleConsumersGroup");
    KafkaDocumentSource<String> receiverChannel2 = setupSource("retryWithMultipleConsumersGroup");
    receiverChannel1.createDocumentSubscription(listener);
    receiverChannel2.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument("initialDoc");
    // wait for initial message consumed and trigger commit to kafka
    assertTrue(semaphore.tryAcquire(1, 20, TimeUnit.SECONDS));

    for (int i = 0; i < 1000; i++) {
      senderChannel.getDocumentChannel().sendDocument("mydoc" + i);
    }
    senderChannel.getDocumentChannel().flush();

    //wait for all 1000 documents to come through
    if (!semaphore.tryAcquire(1000, 20, TimeUnit.SECONDS)) {
      fail("Tried to aquire 1000 documents, but got only " + semaphore.availablePermits());
    }

    for (int i = 0; i < 1000; i++) {
      assertTrue("Missing document mydoc" + i, documents.contains("mydoc" + i));
    }

    //in total, we should have received 1001 documents (1000 OK, and 1 which was rejected)
    verify(listener, times(1001)).documentReceived(argThat(i->i.startsWith("mydoc")));
    //we should have received ONE error
    verify(errorListener).accept(isA(RuntimeException.class));
  }

  @Test
  public void multipleConsumerGroup() throws InterruptedException {
    useTopic("multipleConsumerGroupTest");
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

  private KafkaDocumentDestination<String> setupDestination() {
    KafkaDocumentDestination<String> channel = KafkaDocumentDestination.<String>builder()
            .setProducerProvider(createProducerProvider())
            .setFlushAfterWrite(false)
            .setTopicName(topic)
            .setType(String.class)
            .build();
    channels.add(channel);
    return channel;
  }

  //convenience method to reduce verbosity of doAnswer
  private static Answer asVoid(Consumer<String> task) {
    return i->{
      task.accept(i.getArgument(0));
      return null;
    };
  }

  private KafkaDocumentSource<String> setupSource(String group) {
    return setupSource(group, null, null);
  }

  private KafkaDocumentSource<String> setupSource(String group, Consumer<KafkaDocumentSource.Builder<?>> sourceEdit, Consumer<KafkaConsumerProvider.Builder> providerEdit) {
    // noinspection unchecked
    KafkaDocumentSource.Builder<String> builder = KafkaDocumentSource.<String>builder()
            .setConsumerProvider(createConsumerProvider(group, providerEdit))
            .addErrorListener(errorListener)
            .setTopicName(topic)
            .setType(String.class)
            .setCommitType(KafkaDocumentSource.CommitType.sync);
    ifNotNullDo(sourceEdit, s -> s.accept(builder));
    KafkaDocumentSource<String> channel = builder.build();
    channels.add(channel);
    return channel;
  }

  private KafkaProducerProvider createProducerProvider() {
    return KafkaProducerProvider.builder()
            .setKafkaHosts(kafkaHost())
            .setKafkaPort(kafkaPort())
            .build();
  }

  private KafkaConsumerProvider createConsumerProvider(String group, Consumer<KafkaConsumerProvider.Builder> edits) {
    KafkaConsumerProvider.Builder builder = KafkaConsumerProvider.builder()
            .setMaxPollRecords(2)
            .setKafkaHosts(kafkaHost())
            .setKafkaPort(kafkaPort())
            .setAutoCommit(false)
            .setHeartbeatIntervalMs(1000)
            .setRequestTimeoutMs(11000)
            .setSessionTimeoutMs(10000)
            .setMaxPollIntervalMs(2000)
            .setGroupID(group);
    ifNotNullDo(edits, e -> e.accept(builder));
    return builder.build();
  }

  private String kafkaHost() {
    return DockerTestUtils.getDockerHost();
  }

  private int kafkaPort() {
    return docker.containers()
            .container("kafka")
            .port(9094)
            .getExternalPort();
  }

  private void useTopic(String topic) {
    this.topic = topic;
  }

}
