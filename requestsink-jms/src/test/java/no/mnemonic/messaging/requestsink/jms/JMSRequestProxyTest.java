package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.requestsink.Message.Priority;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.collections.SetUtils.set;
import static no.mnemonic.messaging.requestsink.Message.Priority.*;
import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

public class JMSRequestProxyTest extends AbstractJMSRequestTest {

  @Mock
  private RequestSink endpoint;

  private JMSRequestProxy requestProxy;
  private ComponentContainer container;

  private Queue queue;
  private Topic topic;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown() throws Exception {
    container.destroy();
    if (testConnection != null) testConnection.close();
  }

  @Test
  public void testSignalSubmitsMessage() throws Exception {
    setupEnvironment();
    //listen for signal invocation
    Future<MessageAndContext> expectedSignal = expectSignal();
    //send testmessage
    TestMessage sentMessage = new TestMessage("test1");
    signal(sentMessage, 1000, ProtocolVersion.V3);
    //wait for signal to come through and validate
    TestMessage receivedMessage = expectedSignal.get(1000, TimeUnit.MILLISECONDS).msg;
    assertEquals(sentMessage.getCallID(), receivedMessage.getCallID());
    assertEquals(sentMessage, receivedMessage);
  }

  @Test
  public void testSignalContextEOSReturnsEOSMessage() throws Exception {
    setupEnvironment();
    when(endpoint.signal(any(), any(), anyLong())).thenAnswer(i -> {
      RequestContext ctx = i.getArgument(1);
      ctx.endOfStream();
      return ctx;
    });
    TestMessage sentMessage = new TestMessage("test1");
    Destination responseQueue = signal(sentMessage, 1000, ProtocolVersion.V3);
    Message eosMessage = receiveFrom(responseQueue).poll(1000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED, eosMessage.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    assertEquals(sentMessage.getCallID(), eosMessage.getJMSCorrelationID());
    assertEquals(ProtocolVersion.V3, getProtocolVersion(eosMessage));
  }


  @Test
  public void testResourceIsolationForBulkAndStandardRequest() throws Exception {
    setupEnvironment(e->e
        .setMaxConcurrentCallsBulk(1)
        .setMaxConcurrentCallsStandard(3)
        .setMaxConcurrentCallsExpedite(1)
    );
    CountDownLatch latch = new CountDownLatch(1);
    try {
      //blocking queue, which registers the signals, but keeps them trapped until the latch is count down
      BlockingQueue<TestMessage> signalQueue = getBlockingSignalQueue(latch);

      signal(new TestMessage("standard1").setPriority(standard), 1000, ProtocolVersion.V3);
      signal(new TestMessage("standard2").setPriority(standard), 1000, ProtocolVersion.V3);
      signal(new TestMessage("bulk1").setPriority(bulk), 1000, ProtocolVersion.V3);
      //this message is never accepted by the proxy, because it has run out of concurrent bulk calls
      signal(new TestMessage("bulk2").setPriority(bulk), 1000, ProtocolVersion.V3);
      signal(new TestMessage("expedite1").setPriority(expedite), 1000, ProtocolVersion.V3);
      //this message is never accepted by the proxy, because it has run out of concurrent expedite calls
      signal(new TestMessage("expedite2").setPriority(expedite), 1000, ProtocolVersion.V3);

      //verify four received messages
      Set<String> received = set();
      for (int i = 0; i < 4; i++) {
        TestMessage msg = signalQueue.poll(1000, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        received.add(msg.getId());
      }
      //no more messages received
      assertNull(signalQueue.poll(1000, TimeUnit.MILLISECONDS));
      //verify that we got two standard, one expedite and one bulk message through
      assertEquals(set("bulk1", "standard1", "standard2", "expedite1"), received);
    } finally {
      latch.countDown();
    }
  }

  @Test
  public void testSignalSingleResponse() throws Exception {
    setupEnvironment();
    doTestSignalResponse(1);
  }

  @Test
  public void testSignalMultipleResponses() throws Exception {
    setupEnvironment();
    doTestSignalResponse(100);
  }

  @Test
  public void testExtendWait() throws Exception {
    setupEnvironment();
    when(endpoint.signal(any(), any(), anyLong())).thenAnswer(inv -> {
      RequestContext ctx = inv.getArgument(1);
      for (int i = 0; i < 10; i++) {
        Thread.sleep(500);
        ctx.keepAlive(System.currentTimeMillis() + 1000);
      }
      ctx.addResponse(new TestMessage("resp"), ()->{});
      ctx.endOfStream();
      return ctx;
    });

    TestMessage sentMessage = new TestMessage("test1");
    Destination responseQueue = signal(sentMessage, 1000, ProtocolVersion.V3);
    System.out.println("Timeout at " + new Date(System.currentTimeMillis() + 1000));
    BlockingQueue<Message> response = receiveFrom(responseQueue);

    for (int i = 0; i < 10; i++) {
      Message r = response.poll(1000, TimeUnit.MILLISECONDS);
      assertEquals(JMSRequestProxy.MESSAGE_TYPE_EXTEND_WAIT, r.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
      assertEquals(ProtocolVersion.V3, getProtocolVersion(r));
      assertEquals(sentMessage.getCallID(), r.getJMSCorrelationID());
      long until = r.getLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT);
      assertTrue(until > System.currentTimeMillis());
      System.out.println("Extending until " + new Date(until));
    }
    Message respMessage = response.poll(1000, TimeUnit.MILLISECONDS);
    assertEquals(JMSRequestProxy.MESSAGE_TYPE_SIGNAL_RESPONSE, respMessage.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    assertEquals("resp", ((TestMessage) TestUtils.unserialize(extractMessageBytes(respMessage))).getId());

    Message eosMessage = response.poll(1000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED, eosMessage.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
  }

  @Test
  public void testAbort() throws Exception {
    setupEnvironment();
    Semaphore semaphore = new Semaphore(0);
    when(endpoint.signal(any(), any(), anyLong())).thenAnswer(i->{
      semaphore.release();
      return i.getArgument(1);
    });
    doAnswer(i->{
      semaphore.release();
      return null;
    }).when(endpoint).abort(any());

    TestMessage sentMessage = new TestMessage("test1");
    signal(sentMessage, 1000, ProtocolVersion.V3);
    assertTrue(semaphore.tryAcquire(1, 1, TimeUnit.SECONDS));

    abort(sentMessage.getCallID());
    assertTrue(semaphore.tryAcquire(1, 1, TimeUnit.SECONDS));
    verify(endpoint).abort(sentMessage.getCallID());
  }

  @Test
  public void testChannelUpload() throws Exception {
    setupEnvironment();
    TestMessage sentMessage = new TestMessage("a bit longer message which is fragmented");
    //listen for signal invocation
    Future<MessageAndContext> expectedSignal = expectSignal();
    //request channel
    Destination responseQueue = requestChannel(sentMessage.getCallID(), 1000);
    BlockingQueue<Message> responses = receiveFrom(responseQueue);

    //receive channel setup
    Message channelSetup = responses.poll(1000, TimeUnit.MILLISECONDS);
    assertEquals(JMSRequestProxy.MESSAGE_TYPE_CHANNEL_SETUP, channelSetup.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    assertEquals(sentMessage.getCallID(), channelSetup.getJMSCorrelationID());
    Destination channel = channelSetup.getJMSReplyTo();

    //fragment and upload data through channel
    uploadAndCloseChannel(channel, sentMessage.getCallID(), TestUtils.serialize(sentMessage), 10);

    //wait for signal to come through after upload and validate
    TestMessage receivedMessage = expectedSignal.get(1000, TimeUnit.MILLISECONDS).msg;
    assertEquals(sentMessage.getCallID(), receivedMessage.getCallID());
    assertEquals(sentMessage, receivedMessage);
  }

  @Test
  public void testFragmentedResponse() throws Exception {
    setupEnvironment();
    TestMessage response = createBigResponse();
    String digest = md5(TestUtils.serialize(response));

    TestMessage sentMessage = new TestMessage("request");
    //listen for signal invocation
    Future<MessageAndContext> expectedSignal = expectSignal();
    //request channel
    Destination responseQueue = signal(sentMessage, 1000, ProtocolVersion.V3);
    BlockingQueue<Message> responses = receiveFrom(responseQueue);

    //wait for signal to be received, and send huge response
    expectedSignal.get(1000, TimeUnit.MILLISECONDS).ctx.addResponse(response, ()->{});

    //wait for fragmented reply
    Message firstFragment = responses.poll(100000, TimeUnit.MILLISECONDS);

    assertEquals(JMSRequestProxy.MESSAGE_TYPE_SIGNAL_FRAGMENT, firstFragment.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    assertEquals(0, firstFragment.getIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_IDX));

    Message secondFragment = responses.poll(1000, TimeUnit.MILLISECONDS);
    assertEquals(JMSRequestProxy.MESSAGE_TYPE_SIGNAL_FRAGMENT, secondFragment.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    assertEquals(1, secondFragment.getIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_IDX));

    Message endOfFragments = responses.poll(1000, TimeUnit.MILLISECONDS);
    assertEquals(JMSRequestProxy.MESSAGE_TYPE_END_OF_FRAGMENTED_MESSAGE, endOfFragments.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    assertEquals(2, endOfFragments.getIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_TOTAL));
    assertEquals(digest, endOfFragments.getStringProperty(JMSRequestProxy.PROPERTY_DATA_CHECKSUM_MD5));
  }

  //private methods

  private void doTestSignalResponse(int numberOfResponses) throws NamingException, JMSException, IOException, InterruptedException, ClassNotFoundException {
    respondToSignal(numberOfResponses);

    TestMessage sentMessage = new TestMessage("test1");
    Destination responseQueue = signal(sentMessage, 1000, ProtocolVersion.V3);
    BlockingQueue<Message> response = receiveFrom(responseQueue);

    for (int i = 0; i < numberOfResponses; i++) {
      Message r = response.poll(1000, TimeUnit.MILLISECONDS);
      assertNotNull(r);
      assertEquals(JMSRequestProxy.MESSAGE_TYPE_SIGNAL_RESPONSE, r.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
      assertEquals(ProtocolVersion.V3, getProtocolVersion(r));
      assertEquals(sentMessage.getCallID(), r.getJMSCorrelationID());
      assertEquals("resp" + i, ((TestMessage) TestUtils.unserialize(extractMessageBytes(r))).getId());
    }
    Message eosMessage = response.poll(1000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED, eosMessage.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
  }

  private void respondToSignal(int numberOfResponses) {
    when(endpoint.signal(any(), any(), anyLong())).thenAnswer(inv -> {
      RequestContext ctx = inv.getArgument(1);
      for (int i = 0; i < numberOfResponses; i++) {
        ctx.addResponse(new TestMessage("resp" + i), ()->{});
      }
      ctx.endOfStream();
      return ctx;
    });
  }

  private BlockingQueue<TestMessage> getBlockingSignalQueue(CountDownLatch latch) {
    BlockingQueue<TestMessage> queue = new LinkedBlockingQueue<>();
    when(endpoint.signal(any(), any(), anyLong())).thenAnswer(i -> {
      TestMessage signal = i.getArgument(0);
      RequestContext ctx = i.getArgument(1);
      queue.add(signal);
      latch.await(1000, TimeUnit.MILLISECONDS);
      return ctx;
    });
    return queue;
  }

  private Future<MessageAndContext> expectSignal() {
    CompletableFuture<MessageAndContext> f = new CompletableFuture<>();
    when(endpoint.signal(any(), any(), anyLong())).thenAnswer(i -> {
      TestMessage signal = i.getArgument(0);
      RequestContext ctx = i.getArgument(1);
      f.complete(new MessageAndContext(signal, ctx));
      return ctx;
    });
    return f;
  }

  static class MessageAndContext {
    private TestMessage msg;
    private RequestContext ctx;

    MessageAndContext(TestMessage msg, RequestContext ctx) {
      this.msg = msg;
      this.ctx = ctx;
    }
  }

  private Future<Void> listenForProxyConnection() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    requestProxy.addJMSRequestProxyConnectionListener(l -> future.complete(null));
    return future;
  }

  private BlockingQueue<Message> receiveFrom(Destination destination) throws JMSException {
    BlockingQueue<Message> q = new LinkedBlockingDeque<>();
    MessageConsumer consumer = session.createConsumer(destination);
    consumer.setMessageListener(q::add);
    return q;
  }

  private Destination signal(no.mnemonic.messaging.requestsink.Message msg, long timeout, ProtocolVersion protocolVersion)
          throws JMSException, IOException {
    int priority = determinePriority(msg);
    Destination responseQueue = session.createTemporaryQueue();
    Message message = byteMsg(msg, JMSRequestProxy.MESSAGE_TYPE_SIGNAL, msg.getCallID());
    message.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, System.currentTimeMillis() + timeout);
    message.setStringProperty(JMSRequestProxy.PROTOCOL_VERSION_KEY, protocolVersion.getVersionString());
    message.setJMSReplyTo(responseQueue);
    MessageProducer producer = session.createProducer(queue);
    producer.send(message, DeliveryMode.NON_PERSISTENT, priority, timeout);
    producer.close();
    return responseQueue;
  }

  private int determinePriority(no.mnemonic.messaging.requestsink.Message msg) {
    Priority pri = msg.getPriority();
    if (pri == null) return 4;
    switch (pri) {
      case standard: return 4;
      case bulk: return 1;
      case expedite: return 9;
      default: return 4;
    }
  }

  private void uploadAndCloseChannel(Destination channel, String callID, byte[] data, int maxSize) throws Exception {
    MessageProducer producer = session.createProducer(channel);
    String md5sum = md5(data);
    List<byte[]> fragments = splitArray(data, maxSize);
    int idx = 0;
    for (byte[] f : fragments) {
      Message message = byteMsg(f, JMSRequestProxy.MESSAGE_TYPE_SIGNAL_FRAGMENT, callID);
      message.setIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_IDX, idx++);
      message.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, System.currentTimeMillis() + 10000);
      producer.send(message);
    }
    Message eos = textMsg("channel end", JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED, callID);
    eos.setIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_TOTAL, idx);
    eos.setStringProperty(JMSRequestProxy.PROPERTY_DATA_CHECKSUM_MD5, md5sum);
    producer.send(eos);
    producer.close();
  }

  private Destination requestChannel(String callID, long timeout) throws Exception {
    Destination responseQueue = session.createTemporaryQueue();
    Message message = textMsg("channel request", JMSRequestProxy.MESSAGE_TYPE_CHANNEL_REQUEST, callID);
    message.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, System.currentTimeMillis() + timeout);
    message.setJMSReplyTo(responseQueue);
    MessageProducer producer = session.createProducer(queue);
    producer.send(message);
    producer.close();
    return responseQueue;
  }

  private void abort(String callID) throws Exception {
    Message message = textMsg("channel close request", JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED, callID);
    message.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, System.currentTimeMillis() + 1000);
    MessageProducer producer = session.createProducer(topic);
    producer.send(message);
    producer.close();
  }

  private void createContainer() {
    container = ComponentContainer.create(requestProxy);
    container.initialize();
  }

  @SafeVarargs
  private final void setupProxy(String queueName, String topicName, final Consumer<JMSRequestProxy.Builder>... edits) {
    //set up request sink pointing at a vm-local topic
    JMSRequestProxy.Builder builder = addConnection(JMSRequestProxy.builder())
        .setShutdownTimeout(500)
        .addSerializer(new DefaultJavaMessageSerializer())
        .setQueueName(queueName)
        .setTopicName(topicName)
        .setRequestSink(endpoint)
        .setMaxMessageSize(1000);
    list(edits).forEach(e->e.accept(builder));
    requestProxy = builder.build();
  }

  @SafeVarargs
  private final void setupEnvironment(final Consumer<JMSRequestProxy.Builder>... edits) throws NamingException, JMSException, InterruptedException, ExecutionException, TimeoutException {
    //set up a real JMS connection to a vm-local activemq
    String queueName = "dynamicQueues/" + generateCookie(10);
    String topicName = "dynamicTopics/" + generateCookie(10);

    setupProxy(queueName, topicName, edits);
    Future<Void> proxyConnected = listenForProxyConnection();
    createContainer();

    session = createSession();
    queue = (Queue) lookupDestination(queueName);
    topic = (Topic) lookupDestination(topicName);
    //wait for proxy to connect
    proxyConnected.get(1000, TimeUnit.MILLISECONDS);
  }
}
