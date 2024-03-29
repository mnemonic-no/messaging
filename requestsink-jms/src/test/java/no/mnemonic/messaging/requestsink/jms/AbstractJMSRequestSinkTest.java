package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestListener;
import no.mnemonic.messaging.requestsink.jms.serializer.MessageSerializer;
import no.mnemonic.messaging.requestsink.jms.util.FragmentConsumer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.naming.NamingException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static no.mnemonic.messaging.requestsink.jms.ProtocolVersion.V3;
import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public abstract class AbstractJMSRequestSinkTest extends AbstractJMSRequestTest {

  @Mock
  protected RequestContext requestContext;

  protected JMSRequestSink requestSink;
  protected ComponentContainer container;

  protected Future<Void> endOfStream;
  protected BlockingQueue<Message> queue;
  protected BlockingQueue<Message> topic;
  protected String queueName;
  protected String topicName;
  protected AtomicReference<RequestListener> requestListener = new AtomicReference<>();


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    queueName = "dynamicQueues/" + generateCookie(10);
    topicName = "dynamicTopics/" + generateCookie(10);
    session = createSession();
    endOfStream = expectEndOfStream();
    queue = receiveFrom(queueName);
    topic = receiveFrom(topicName);

    doAnswer(i -> {
      requestListener.set(i.getArgument(0));
      return null;
    }).when(requestContext).addListener(any());
  }

  @After
  public void tearDown() throws Exception {
    container.destroy();
    if (testConnection != null) testConnection.close();
  }

  protected abstract MessageSerializer serializer() throws IOException;

  @Test
  public void testSignalRegisteresRequestListener() throws Exception {
    setupSinkAndContainer();
    requestSink.signal(new TestMessage("test1"), requestContext, 10000);
    assertNotNull(requestListener.get());
  }

  @Test
  public void testSignalNotifiesRequestListenerOnClose() throws Exception {
    setupSinkAndContainer();
    requestSink.setCleanupInSeparateThread(false);
    when(requestContext.isClosed()).thenReturn(true);
    requestSink.signal(new TestMessage("test1"), requestContext, 10000);
    verify(requestContext).notifyClose();
  }

  @Test
  public void testAbortNotifiesProxy() throws Exception {
    setupSinkAndContainer();
    requestSink.setCleanupInSeparateThread(false);
    TestMessage msg = new TestMessage("test1");
    requestSink.signal(msg, requestContext, 10000);
    requestSink.abort(msg.getCallID());
    Message firstMessage = expectSignal();
    Message secondMessage = expectTopicMessage(JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED);
    assertEquals(firstMessage.getJMSCorrelationID(), secondMessage.getJMSCorrelationID());
  }

  @Test
  public void testSignalSubmitsMessage() throws Exception {
    setupSinkAndContainer();
    TestMessage testMessage = new TestMessage("test1");
    //send testmessage
    requestSink.signal(testMessage, requestContext, 10000);
    //wait for message to come through and validate
    Message receivedMessage = expectSignal();
    Assert.assertEquals(ProtocolVersion.V3.getVersionString(), receivedMessage.getStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY));
    Assert.assertEquals(JMSRequestProxy.MESSAGE_TYPE_SIGNAL, receivedMessage.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    assertEquals(testMessage, serializer().deserialize(extractMessageBytes(receivedMessage), getClass().getClassLoader()));
    assertTrue(receivedMessage instanceof BytesMessage);
  }

  @Test
  public void testAbortSubmitsSignal() throws Exception {
    setupSinkAndContainer();
    //send testmessage
    requestSink.abort("abortedCallID");
    //wait for message to come through and validate
    Message receivedMessage = expectTopicMessage(JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED);
    Assert.assertEquals(ProtocolVersion.V3.getVersionString(), receivedMessage.getStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY));
    assertEquals("abortedCallID", receivedMessage.getJMSCorrelationID());
    assertTrue(receivedMessage instanceof TextMessage);
  }

  @Test
  public void testTimeoutNotificationTriggersResponseQueueChange() throws Exception {
    setupSinkAndContainer();

    //send first message
    requestSink.signal(new TestMessage("test1"), requestContext, 10000);
    Message msg1 = expectSignal();

    //notify timeout
    requestListener.get().timeout();

    requestSink.signal(new TestMessage("test2"), requestContext, 10000);
    Message msg2 = expectSignal();

    //verify that response queue has changed
    assertNotEquals(msg1.getJMSReplyTo(), msg2.getJMSReplyTo());
  }

  @Test
  public void testInvalidatedResponseQueueNotClosedUntilAllPendingRequestsAreClosed() throws Exception {
    setupSinkAndContainer();
    requestSink.setCleanupInSeparateThread(false);

    when(requestContext.isClosed()).thenReturn(false);
    requestSink.signal(new TestMessage("test1"), requestContext, 10000);
    Message signal = expectSignal();
    //send reply to response queue to check that it is not removed
    reply(signal, new TestMessage("obj"));

    //notify timeout, marking current response queue as invalid
    requestListener.get().timeout();

    //send another signal (should trigger cleanup of closed request and cleanup of invalid response queue)
    requestSink.signal(new TestMessage("test2"), requestContext, 10000);
    //send reply to response queue to check that it is not removed
    reply(signal, new TestMessage("obj"));

    //close ongoing requests, triggering a cleanup
    when(requestContext.isClosed()).thenReturn(true);
    requestSink.signal(new TestMessage("test3"), requestContext, 10000);
    assertFalse(responseQueueIsValid(signal));
  }

  @Test
  public void testRequestSinkIsClosedOnCleanup() throws Exception {
    setupSinkAndContainer();
    requestSink.setCleanupInSeparateThread(false);

    when(requestContext.isClosed()).thenReturn(false);
    requestSink.signal(new TestMessage("test1"), requestContext, 10000);
    verify(requestContext, never()).notifyClose();

    //when this mock returns isClosed(), then both signals will be closed and notified
    when(requestContext.isClosed()).thenReturn(true);
    requestSink.signal(new TestMessage("test1"), requestContext, 10000);
    verify(requestContext, times(2)).notifyClose();
  }

  @Test
  public void testSignalReceiveSingleResult() throws Exception {
    doTestSignalReceiveResults(1);
  }

  @Test
  public void testSignalReceiveMultipleResults() throws Exception {
    doTestSignalReceiveResults(100);
  }

  @Test
  public void testSignalReceivesFragmentedResponse() throws Exception {
    setupSinkAndContainer();
    TestMessage testMessage = new TestMessage("test1");
    TestMessage responseMessage = new TestMessage("huge response");

    //signal message
    requestSink.signal(testMessage, requestContext, 10000);
    //wait for message to come through
    Message receivedMessage = expectSignal();
    UUID responseID = UUID.randomUUID();

    //fragment response message into fragments of 3 bytes
    fragment(new ByteArrayInputStream(serializer().serialize(responseMessage)), 3, new FragmentConsumer() {
      @Override
      public void fragment(byte[] data, int idx) throws JMSException, IOException {
        try {
          //each fragment is marked with responseID, fragment index and type "signal fragment"
          BytesMessage msg = byteMsg(data, JMSRequestProxy.MESSAGE_TYPE_SIGNAL_FRAGMENT, receivedMessage.getJMSCorrelationID());
          msg.setIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_IDX, idx);
          msg.setStringProperty(JMSRequestProxy.PROPERTY_RESPONSE_ID, responseID.toString());
          sendTo(receivedMessage.getJMSReplyTo(), msg);
        } catch (NamingException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void end(int fragments, byte[] digest) {
        try {
          //end-of-fragment message contains responseID, fragment count and md5 checksum
          sendTo(
                  receivedMessage.getJMSReplyTo(),
                  textMsg("end-of-fragments", JMSRequestProxy.MESSAGE_TYPE_END_OF_FRAGMENTED_MESSAGE, receivedMessage.getJMSCorrelationID(),
                          msg -> msg.setStringProperty(JMSRequestProxy.PROPERTY_RESPONSE_ID, responseID.toString()),
                          msg -> msg.setIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_TOTAL, fragments),
                          msg -> msg.setStringProperty(JMSRequestProxy.PROPERTY_DATA_CHECKSUM_MD5, hex(digest))
                  )
          );
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    //end of stream for the request
    eos(receivedMessage);
    //wait for EOS
    waitForEOS();
    //verify that the response got through as well
    verify(requestContext).addResponse(eq(responseMessage), any());
  }


  @Test
  public void testSignalKeepalive() throws Exception {
    setupSinkAndContainer();
    TestMessage testMessage = new TestMessage("test1");
    //signal message with short lifetime
    requestSink.signal(testMessage, requestContext, 1000);
    //wait for message to come through
    Message receivedMessage = expectSignal();
    keepalive(receivedMessage, 1000);
    eos(receivedMessage);
    waitForEOS();
    verify(requestContext).keepAlive(1000);
  }

  @Test
  public void testChannelUploadWithResponse() throws Exception {
    setupSinkAndContainer(b -> b.setMaxMessageSize(100));

    TestMessage testMessage = new TestMessage(generateCookie(500));
    requestSink.signal(testMessage, requestContext, 1000);

    //expect channel request
    Message receivedMessage = expectQueueMessage(JMSRequestProxy.MESSAGE_TYPE_CHANNEL_REQUEST);
    ChannelUploadVerifier verifier = setupChannel(receivedMessage, testMessage);
    verifier.waitForEOS();
    verifier.verify();

    //when channel has uploaded, send response
    reply(receivedMessage, new TestMessage("response"));
    eos(receivedMessage);

    waitForEOS();
    verify(requestContext, atLeastOnce()).keepAlive(anyLong());
    verify(requestContext).addResponse(eq(new TestMessage("response")), any());
    verify(requestContext).endOfStream();
  }


  //helper methods
  private void doTestSignalReceiveResults(int resultCount) throws Exception {
    setupSinkAndContainer();
    TestMessage testMessage = new TestMessage("test1");
    TestMessage responseMessage = new TestMessage("response");

    //signal message
    requestSink.signal(testMessage, requestContext, 10000);
    //wait for message to come through
    Message receivedMessage = expectSignal();
    //send reply and EOS
    for (int i = 0; i < resultCount; i++) {
      reply(receivedMessage, responseMessage);
    }
    eos(receivedMessage);
    waitForEOS();
    //verify that the response got through as well
    verify(requestContext, times(resultCount)).addResponse(eq(responseMessage), any());
  }

  @SafeVarargs
  private final void setupSinkAndContainer(Consumer<JMSRequestSink.Builder>... changes) throws IOException {
    JMSRequestSink.Builder builder = createBaseSink();
    ListUtils.list(changes).forEach(c -> c.accept(builder));
    requestSink = builder.build();
    container = ComponentContainer.create(requestSink);
    container.initialize();
  }

  protected JMSRequestSink.Builder createBaseSink() throws IOException {
    return addConnection(JMSRequestSink.builder())
            .setSerializer(serializer())
            .setQueueName(queueName)
            .setTopicName(topicName)
            .setProtocolVersion(V3)
            .setMaxMessageSize(65536);
  }

  private void waitForEOS() throws Exception {
    endOfStream.get(1000, TimeUnit.MILLISECONDS);
  }

  private Message expectSignal() throws InterruptedException, JMSException {
    return expectMessage(queue, JMSRequestProxy.MESSAGE_TYPE_SIGNAL);
  }

  private Message expectQueueMessage(String messageType) throws InterruptedException, JMSException {
    return expectMessage(queue, messageType);
  }

  private Message expectTopicMessage(String messageType) throws InterruptedException, JMSException {
    return expectMessage(topic, messageType);
  }

  private Message expectMessage(BlockingQueue<Message> source, String messageType) throws InterruptedException, JMSException {
    Message receivedMessage = source.poll(1000, TimeUnit.MILLISECONDS);
    assertNotNull(receivedMessage);
    assertEquals(messageType, receivedMessage.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    return receivedMessage;
  }

  private boolean responseQueueIsValid(Message signal) {
    try {
      //send reply to response queue and expect exception because it is removed
      reply(signal, new TestMessage("obj"));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private ChannelUploadVerifier setupChannel(Message receivedMessage, Object expectedMessage) throws Exception {
    TemporaryQueue uploadQueue = session.createTemporaryQueue();
    MessageConsumer consumer = session.createConsumer(uploadQueue);
    ChannelUploadVerifier verifier = new ChannelUploadVerifier(expectedMessage);
    consumer.setMessageListener(verifier::receive);
    sendTo(receivedMessage.getJMSReplyTo(),
            textMsg("channel setup", JMSRequestProxy.MESSAGE_TYPE_CHANNEL_SETUP, receivedMessage.getJMSCorrelationID(),
                    m -> m.setJMSReplyTo(uploadQueue)));
    return verifier;
  }

  private void eos(Message msg) throws Exception {
    sendTo(msg.getJMSReplyTo(), textMsg("eos", JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED, msg.getJMSCorrelationID()));
  }

  private void keepalive(Message msg, long until) throws Exception {
    sendTo(msg.getJMSReplyTo(), textMsg("keepalive", JMSRequestProxy.MESSAGE_TYPE_EXTEND_WAIT, msg.getJMSCorrelationID(),
            m -> m.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, until)));
  }

  private void reply(Message msg, no.mnemonic.messaging.requestsink.Message obj) throws Exception {
    sendTo(msg.getJMSReplyTo(), byteMsg(obj, JMSRequestProxy.MESSAGE_TYPE_SIGNAL_RESPONSE, msg.getJMSCorrelationID()));
  }

  private void sendTo(Destination destination, Message msg) throws NamingException, JMSException {
    try (MessageProducer producer = session.createProducer(destination)) {
      producer.send(msg);
    }
  }

  @Override
  BytesMessage byteMsg(no.mnemonic.messaging.requestsink.Message obj, String messageType, String callID) throws JMSException, IOException {
    BytesMessage msg = session.createBytesMessage();
    msg.writeBytes(serializer().serialize(obj));
    msg.setStringProperty(AbstractJMSRequestBase.SERIALIZER_KEY, serializer().serializerID());
    msg.setStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY, ProtocolVersion.V3.getVersionString());
    msg.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, messageType);
    msg.setJMSCorrelationID(callID);
    return msg;
  }

  private class ChannelUploadVerifier {
    final Object expectedObject;

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final MessageDigest digest = md5();
    int fragmentCount = 0;
    CompletableFuture<Void> eos = new CompletableFuture<>();

    ChannelUploadVerifier(Object expectedObject) {
      this.expectedObject = expectedObject;
    }

    void waitForEOS() throws Exception {
      eos.get(1000, TimeUnit.MILLISECONDS);
    }

    void verify() throws Exception {
      assertTrue(eos.isDone());
      //verify that received data is as expected
      assertEquals(expectedObject, serializer().deserialize(baos.toByteArray(), AbstractJMSRequestSinkTest.this.getClass().getClassLoader()));
    }

    void receive(Message msg) {
      try {
        assertFalse(eos.isDone());
        String messageType = msg.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE);
        //expect stream to terminate with an EOS message
        if (messageType.equals(JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED)) {
          baos.close();
          //make sure fragment count and checksum are correct
          assertEquals(msg.getIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_TOTAL), fragmentCount);
          String checksum = msg.getStringProperty(JMSRequestProxy.PROPERTY_DATA_CHECKSUM_MD5);
          assertEquals(checksum, hex(digest.digest()));
          eos.complete(null);
        } else {
          BytesMessage bmsg = (BytesMessage) msg;
          fragmentCount++;
          byte[] data = new byte[(int) bmsg.getBodyLength()];
          bmsg.readBytes(data);
          baos.write(data);
          digest.update(data);
        }
      } catch (JMSException | IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Future<Void> expectEndOfStream() {
    CompletableFuture<Void> eos = new CompletableFuture<>();
    doAnswer(i -> eos.complete(null)).when(requestContext).endOfStream();
    return eos;
  }

  private BlockingQueue<Message> receiveFrom(String destination) throws NamingException, JMSException {
    return receiveFrom(lookupDestination(destination));
  }

  private BlockingQueue<Message> receiveFrom(Destination destination) throws JMSException {
    BlockingQueue<Message> result = new LinkedBlockingDeque<>();
    MessageConsumer consumer = session.createConsumer(destination);
    consumer.setMessageListener(result::add);
    return result;
  }


}
