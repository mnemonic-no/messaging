package no.mnemonic.messaging.documentchannel.jms;

import no.mnemonic.commons.testtools.AvailablePortFinder;
import no.mnemonic.messaging.documentchannel.DocumentBatch;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class JMSDocumentChannelTest {

  private BrokerService broker;
  private int port = AvailablePortFinder.getAvailablePort(10000);
  @Mock
  private DocumentChannelListener<String> listener;
  private Semaphore semaphore = new Semaphore(0);

  private Collection<AutoCloseable> channels = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    startBroker();

    doAnswer(i -> {
      semaphore.release();
      return null;
    }).when(listener).documentReceived(any());
  }

  @After
  public void tearDown() throws Exception {
    channels.forEach(c -> tryTo(c::close));
    broker.stop();
    broker.waitUntilStopped();
  }

  @Test
  public void subscriberQueue() throws InterruptedException {
    JMSDocumentDestination<String> senderChannel = setupDestination("dynamicQueues/myqueue", false);
    JMSDocumentSource<String> receiverChannel1 = setupSource("dynamicQueues/myqueue", false, false);
    JMSDocumentSource<String> receiverChannel2 = setupSource("dynamicQueues/myqueue", false, false);
    receiverChannel1.createDocumentSubscription(listener);
    receiverChannel2.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument("mydoc");
    assertTrue(semaphore.tryAcquire(1, TimeUnit.SECONDS));
    verify(listener).documentReceived("mydoc");
  }

  @Test
  public void receiveFromPoll() {
    JMSDocumentDestination<String> senderChannel = setupDestination("dynamicQueues/myqueue", false);
    JMSDocumentSource<String> receiverChannel1 = setupSource("dynamicQueues/myqueue", false, false);

    senderChannel.getDocumentChannel().sendDocument("mydoc");
    assertEquals(list("mydoc"), list(receiverChannel1.poll(1, TimeUnit.SECONDS).getDocuments()));
  }

  @Test
  public void receiveFromPollWithoutAck() {
    JMSDocumentDestination<String> senderChannel = setupDestination("dynamicQueues/myqueue", false);

    JMSDocumentSource<String> receiverChannel1 = setupSource("dynamicQueues/myqueue", false, true);

    senderChannel.getDocumentChannel().sendDocument("mydoc");
    senderChannel.getDocumentChannel().sendDocument("mydoc2");

    receiverChannel1.poll(1, TimeUnit.SECONDS);
    receiverChannel1.close();

    JMSDocumentSource<String> receiverChannel2 = setupSource("dynamicQueues/myqueue", false, true);
    DocumentBatch<String> batch = receiverChannel2.poll(1, TimeUnit.SECONDS);
    assertEquals(list("mydoc"), list(batch.getDocuments()));
    batch.acknowledge();

    assertEquals(list("mydoc2"), list(receiverChannel2.poll(1, TimeUnit.SECONDS).getDocuments()));
  }

  @Test
  public void subscriberTopic() throws InterruptedException {
    JMSDocumentDestination<String> senderChannel = setupDestination("dynamicTopics/mytopic", false);
    JMSDocumentSource<String> receiverChannel1 = setupSource("dynamicTopics/mytopic", false, false);
    JMSDocumentSource<String> receiverChannel2 = setupSource("dynamicTopics/mytopic", false, false);
    receiverChannel1.createDocumentSubscription(listener);
    receiverChannel2.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument("mydoc");
    assertTrue(semaphore.tryAcquire(2, 1, TimeUnit.SECONDS));
    verify(listener, times(2)).documentReceived("mydoc");
  }

  @Test
  public void subscriberStaysConnectedWithFailover() throws Exception {
    JMSDocumentDestination<String> senderChannel = setupDestination("dynamicQueues/myqueue", true);
    JMSDocumentSource<String> receiverChannel = setupSource("dynamicQueues/myqueue", true, false);
    receiverChannel.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument("mydoc1");
    assertTrue(semaphore.tryAcquire(1, TimeUnit.SECONDS));

    restartBroker();

    senderChannel.getDocumentChannel().sendDocument("mydoc2");
    assertTrue(semaphore.tryAcquire(1, TimeUnit.SECONDS));

    verify(listener).documentReceived("mydoc1");
    verify(listener).documentReceived("mydoc2");

    assertEquals(0, receiverChannel.getConnectionExceptions());
  }

  @Test
  public void subscriberReconnectsWithoutFailover() throws Exception {
    JMSDocumentDestination<String> senderChannel = setupDestination("dynamicQueues/myqueue", false);
    JMSDocumentSource<String> receiverChannel = setupSource("dynamicQueues/myqueue", false, false);
    receiverChannel.createDocumentSubscription(listener);

    senderChannel.getDocumentChannel().sendDocument("mydoc1");
    assertTrue(semaphore.tryAcquire(1, TimeUnit.SECONDS));
    verify(listener).documentReceived("mydoc1");

    restartBroker();

    boolean recovered = false;
    while (!recovered) {
      senderChannel.getDocumentChannel().sendDocument("mydoc2");
      recovered = semaphore.tryAcquire(1, TimeUnit.SECONDS);
    }
    verify(listener).documentReceived("mydoc2");

    assertEquals(1, receiverChannel.getConnectionExceptions());
  }

  @Test
  public void senderProducerFailure() throws Exception {
    JMSDocumentSource<String> receiverChannel = setupSource("dynamicQueues/myqueue", true, false);
    receiverChannel.createDocumentSubscription(listener);

    JMSDocumentDestination<String> senderChannel = setupDestination("dynamicQueues/myqueue", false);

    senderChannel.getDocumentChannel().sendDocument("docbeforerestart");
    assertEquals(0, senderChannel.getProducerExceptions());

    restartBroker();

    boolean recovered = false;
    while (!recovered) {
      senderChannel.getDocumentChannel().sendDocument("docafterrestart");
      recovered = semaphore.tryAcquire(1, TimeUnit.SECONDS);
    }

    assertEquals(1, senderChannel.getProducerExceptions());
    verify(listener).documentReceived("docafterrestart");
  }

  //helper methods

  private void startBroker() throws Exception {
    broker = new BrokerService();
    broker.setPersistent(false);
    broker.addConnector("tcp://0.0.0.0:" + port);
    broker.start();
    broker.waitUntilStarted();
    System.out.println("Broker started on port " + port);
  }

  private void restartBroker() throws Exception {
    broker.stop();
    broker.waitUntilStopped();
    System.out.println("Broker stopped");
    startBroker();
  }

  private JMSSession createSession(String destination, boolean failover, boolean clientAck) {
    String contextURL = "tcp://localhost:" + port;
    if (failover) {
      contextURL = String.format("failover:(tcp://localhost:%d)?initialReconnectDelay=100&maxReconnectAttempts=2&timeout=100", port);
    }
    try {
      return JMSSession.builder()
              .setContextURL(contextURL)
              .setAcknowledgeMode(clientAck ? JMSSession.AcknowledgeMode.client : JMSSession.AcknowledgeMode.auto)
              .setDestination(destination)
              .build();
    } catch (NamingException | JMSException e) {
      throw new IllegalStateException("Error", e);
    }
  }

  private JMSDocumentSource<String> setupSource(String destination, boolean failover, boolean clientAck) {
    JMSDocumentSource<String> channel = JMSDocumentSource.<String>builder()
            .setSessionProvider(() -> createSession(destination, failover, clientAck))
            .setType(String.class)
            .build();
    channels.add(channel);
    return channel;
  }

  private JMSDocumentDestination<String> setupDestination(String destination, boolean failover) {
    JMSDocumentDestination<String> channel = JMSDocumentDestination.<String>builder()
            .setSessionProvider(() -> createSession(destination, failover, false))
            .setType(String.class)
            .build();
    channels.add(channel);
    return channel;
  }

}
