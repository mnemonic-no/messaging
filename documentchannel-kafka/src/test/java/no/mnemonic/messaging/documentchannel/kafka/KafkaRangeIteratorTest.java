package no.mnemonic.messaging.documentchannel.kafka;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import no.mnemonic.commons.junit.docker.DockerTestUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.messaging.documentchannel.DocumentChannel;
import no.mnemonic.messaging.documentchannel.DocumentDestination;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static org.junit.Assert.assertEquals;

public class KafkaRangeIteratorTest {

  @Mock
  private Consumer<Exception> errorListener;

  private Collection<AutoCloseable> channels = new ArrayList<>();
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

  @Test
  public void iterateRange() throws KafkaInvalidSeekException {
    DocumentDestination<String> destination = setupDestination();
    DocumentChannel<String> documentChannel = destination.getDocumentChannel();

    for (int i = 0; i < 100; i++) {
      documentChannel.sendDocument("doc" + i);
    }
    documentChannel.flush();

    KafkaDocumentSource<String> source = setupSource("group", s->s.setCommitType(KafkaDocumentSource.CommitType.none), null);
    KafkaDocumentBatch<String> batch = source.poll(Duration.ofSeconds(2));

    List<KafkaDocument<String>> documents = list(batch.getKafkaDocuments());
    KafkaDocument<String> firstDocument = documents.get(0);
    KafkaDocument<String> lastDocument = documents.get(documents.size()-1);
    assertEquals("doc0", firstDocument.getDocument());
    List<String> allDocuments = ListUtils.list(documents, KafkaDocument::getDocument);
    source.close();

    source = setupSource("group", s->s.setCommitType(KafkaDocumentSource.CommitType.none), null);
    KafkaRangeIterator<String> iterator = new KafkaRangeIterator<>(source, firstDocument.getCursor(), lastDocument.getCursor());
    List<String> replayedDocuments = ListUtils.list(iterator, KafkaDocument::getDocument);

    assertEquals(allDocuments, replayedDocuments);
  }

  @Test
  public void rangeIterateWithIterator() throws KafkaInvalidSeekException, InterruptedException {
    useTopic("rangeIterateWithIteratorTest");
    KafkaDocumentDestination<String> senderChannel = setupDestination();

    for (int i = 0; i<200; i++) {
      senderChannel.getDocumentChannel().sendDocument("mydoc" + i);
      Thread.sleep(1);
    }
    senderChannel.getDocumentChannel().flush();

    KafkaDocumentSource<String> receiverChannel1 = setupSource("group", null, b->b.setMaxPollRecords(50));
    List<KafkaDocument<String>> batch1 = ListUtils.list(receiverChannel1.poll(Duration.ofSeconds(10)).getKafkaDocuments());
    assertEquals("mydoc49", batch1.get(49).getDocument());

    List<KafkaDocument<String>> batch2 = ListUtils.list(receiverChannel1.poll(Duration.ofSeconds(10)).getKafkaDocuments());
    assertEquals("mydoc99", batch2.get(49).getDocument());

    List<KafkaDocument<String>> batch3 = ListUtils.list(receiverChannel1.poll(Duration.ofSeconds(10)).getKafkaDocuments());
    assertEquals("mydoc149", batch3.get(49).getDocument());

    KafkaDocumentSource<String> receiverChannel2 = setupSource("group", null, b->b.setMaxPollRecords(50));

    Iterator<KafkaDocument<String>> rangeIterator = new KafkaRangeIterator<>(receiverChannel2, batch1.get(49).getCursor(), batch2.get(49).getCursor());
    KafkaDocument<String> last = rangeIterator.next();
    assertEquals("mydoc49", last.getDocument());
    while (rangeIterator.hasNext()) {
      last = rangeIterator.next();
    }
    assertEquals("mydoc99", last.getDocument());
  }

  @Test
  public void noCommitWithRangeIterator() throws InterruptedException, KafkaInvalidSeekException {
    useTopic("noCommitWithRangeIteratorTest");
    KafkaDocumentDestination<String> senderChannel = setupDestination();

    for (int i = 0; i<100; i++) {
      senderChannel.getDocumentChannel().sendDocument("mydoc" + i);
      Thread.sleep(1);
    }
    senderChannel.getDocumentChannel().flush();

    //read all documents to find last cursor
    KafkaDocumentSource<String> receiverChannel1 = setupSource("group", s->s.setCommitType(KafkaDocumentSource.CommitType.none), b->b.setMaxPollRecords(500));
    List<KafkaDocument<String>> batch1 = ListUtils.list(receiverChannel1.poll(Duration.ofSeconds(10)).getKafkaDocuments());
    assertEquals(100, batch1.size());
    String endCursor = batch1.get(99).getCursor();
    receiverChannel1.close();

    //use range iterator to iterate all documents
    {
      KafkaDocumentSource<String> receiverChannel2 = setupSource("group2", s->s.setCommitType(KafkaDocumentSource.CommitType.none), null);
      Iterator<KafkaDocument<String>> iter2 = new KafkaRangeIterator<>(receiverChannel2, null, endCursor);
      KafkaDocument<String> last = iter2.next();
      assertEquals("mydoc0", last.getDocument());
      while (iter2.hasNext()) last = iter2.next();
      assertEquals("mydoc99", last.getDocument());
      receiverChannel2.close();
    }
    //setup new iterator with same group, verify that it still reads from the start (no commit has occurred)
    {
      KafkaDocumentSource<String> receiverChannel2 = setupSource("group");
      Iterator<KafkaDocument<String>> iter2 = new KafkaRangeIterator<>(receiverChannel2, null, endCursor);
      assertEquals("mydoc0", iter2.next().getDocument());
    }
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
    ifNotNullDo(sourceEdit, s->s.accept(builder));
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
    ifNotNullDo(edits, e->e.accept(builder));
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