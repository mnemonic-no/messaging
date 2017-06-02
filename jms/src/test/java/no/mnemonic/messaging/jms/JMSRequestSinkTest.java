package no.mnemonic.messaging.jms;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.api.SignalContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.util.concurrent.*;

import static no.mnemonic.messaging.jms.JMSBase.PROTOCOL_VERSION_13;
import static no.mnemonic.messaging.jms.JMSBase.PROTOCOL_VERSION_16;
import static no.mnemonic.messaging.jms.JMSBase.PROTOCOL_VERSION_KEY;
import static no.mnemonic.messaging.jms.JMSRequestProxy.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class JMSRequestSinkTest extends AbstractJMSRequestTest {

  @Mock
  private SignalContext signalContext;

  private JMSRequestSink requestSink;
  private ComponentContainer container;

  private Future<Void> endOfStream;
  private BlockingQueue<Message> queue;
  private JMSConnection connection;
  private String queueName;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    connection = createConnection();
    queueName = "dynamicQueues/" + generateCookie(10);
    session = createSession(false);
    endOfStream = expectEndOfStream();
    queue = receiveFrom(queueName);
  }

  @After
  public void tearDown() throws Exception {
    container.destroy();
    if (testConnection != null) testConnection.close();
  }

  @Test
  public void testSignalSubmitsMessageV13Protocol() throws Exception {
    doTestSignalSubmitsMessage(false);
  }

  @Test
  public void testSignalSubmitsMessageV16Protocol() throws Exception {
    doTestSignalSubmitsMessage(true);
  }

  @Test
  public void testSignalReceiveSingleResultV13Protocol() throws Exception {
    doTestSignalReceiveResults(1, false);
  }

  @Test
  public void testSignalReceiveMultipleResultsV13Protocol() throws Exception {
    doTestSignalReceiveResults(100, false);
  }

  @Test
  public void testSignalReceiveSingleResultV16Protocol() throws Exception {
    doTestSignalReceiveResults(1, true);
  }

  @Test
  public void testSignalReceiveMultipleResultsV16Protocol() throws Exception {
    doTestSignalReceiveResults(100, true);
  }

  @Test
  public void testSignalKeepaliveV13() throws Exception {
    doTestSignalKeepalive(false);
  }

  @Test
  public void testSignalKeepaliveV16() throws Exception {
    doTestSignalKeepalive(true);
  }

  @Test
  public void testChannelUploadWithResponse() throws Exception {
    setupSinkAndContainer(true, 100);

    TestMessage testMessage = new TestMessage(generateCookie(500));
    requestSink.signal(testMessage, signalContext, 1000);

    //expect channel request
    Message receivedMessage = expectMessage(MESSAGE_TYPE_CHANNEL_REQUEST);
    ChannelUploadVerifier verifier = setupChannel(receivedMessage, testMessage);
    verifier.waitForEOS();
    verifier.verify();

    //when channel has uploaded, send response
    reply(receivedMessage, new TestMessage("response"), true);
    eos(receivedMessage, true);

    waitForEOS();
    verify(signalContext).addResponse(eq(new TestMessage("response")));
    verify(signalContext).endOfStream();
  }

  //helper methods

  private void doTestSignalKeepalive(boolean v16) throws Exception {
    setupSinkAndContainer(v16, 65536);
    TestMessage testMessage = new TestMessage("test1");
    //signal message with short lifetime
    requestSink.signal(testMessage, signalContext, 1000);
    //wait for message to come through
    Message receivedMessage = expectMessage(MESSAGE_TYPE_SIGNAL);
    keepalive(receivedMessage, 1000, v16);
    eos(receivedMessage, v16);
    waitForEOS();
    verify(signalContext).keepAlive(1000);
  }

  private void doTestSignalSubmitsMessage(boolean v16) throws Exception {
    setupSinkAndContainer(v16, 65536);
    TestMessage testMessage = new TestMessage("test1");
    //send testmessage
    requestSink.signal(testMessage, signalContext, 10000);
    //wait for message to come through and validate
    Message receivedMessage = expectMessage(MESSAGE_TYPE_SIGNAL);
    assertEquals(v16 ? PROTOCOL_VERSION_16 : PROTOCOL_VERSION_13, receivedMessage.getStringProperty(PROTOCOL_VERSION_KEY));
    assertEquals(MESSAGE_TYPE_SIGNAL, receivedMessage.getStringProperty(PROPERTY_MESSAGE_TYPE));
    assertEquals(testMessage, JMSUtils.extractObject(receivedMessage));
    assertEquals(v16, receivedMessage instanceof BytesMessage);
    assertEquals(!v16, receivedMessage instanceof ObjectMessage);
  }

  private void doTestSignalReceiveResults(int resultCount, boolean v16) throws Exception {
    setupSinkAndContainer(v16, 65536);
    TestMessage testMessage = new TestMessage("test1");
    TestMessage responseMessage = new TestMessage("response");

    //signal message
    requestSink.signal(testMessage, signalContext, 10000);
    //wait for message to come through
    Message receivedMessage = expectMessage(MESSAGE_TYPE_SIGNAL);
    //send reply and EOS
    for (int i = 0; i < resultCount; i++) {
      reply(receivedMessage, responseMessage, v16);
    }
    eos(receivedMessage, v16);
    waitForEOS();
    //verify that the response got through as well
    verify(signalContext, times(resultCount)).addResponse(eq(responseMessage));
  }

  private void setupSinkAndContainer(boolean v16, int maxMessageSize) {
    requestSink = JMSRequestSink.builder()
            .addConnection(connection)
            .setDestinationName(queueName)
            .setMaxMessageSize(maxMessageSize)
            .setProtocolVersion(v16 ? ProtocolVersion.V16 : ProtocolVersion.V13)
            .build();
    container = ComponentContainer.create(requestSink, connection);
    container.initialize();
  }

  private void waitForEOS() throws Exception {
    endOfStream.get(1000, TimeUnit.MILLISECONDS);
  }

  private Message expectMessage(String messageType) throws InterruptedException, JMSException {
    return expectMessage(queue, messageType);
  }

  private Message expectMessage(BlockingQueue<Message> queue, String messageType) throws InterruptedException, JMSException {
    Message receivedMessage = queue.poll(1000, TimeUnit.MILLISECONDS);
    assertNotNull(receivedMessage);
    assertEquals(messageType, receivedMessage.getStringProperty(PROPERTY_MESSAGE_TYPE));
    return receivedMessage;
  }

  private Future<Void> expectEndOfStream() {
    CompletableFuture<Void> eos = new CompletableFuture<>();
    doAnswer(i -> eos.complete(null)).when(signalContext).endOfStream();
    return eos;
  }

  private BlockingQueue<Message> receiveFrom(String destination) throws NamingException, JMSException {
    return receiveFrom(createDestination(destination));
  }

  private BlockingQueue<Message> receiveFrom(Destination destination) throws NamingException, JMSException {
    BlockingQueue<Message> result = new LinkedBlockingDeque<>();
    MessageConsumer consumer = session.createConsumer(destination);
    consumer.setMessageListener(result::add);
    return result;
  }

  private ChannelUploadVerifier setupChannel(Message receivedMessage, Object expectedMessage) throws Exception {
    TemporaryQueue uploadQueue = session.createTemporaryQueue();
    MessageConsumer consumer = session.createConsumer(uploadQueue);
    ChannelUploadVerifier verifier = new ChannelUploadVerifier(expectedMessage);
    consumer.setMessageListener(verifier::receive);
    sendTo(receivedMessage.getJMSReplyTo(),
            textMsg("channel setup", MESSAGE_TYPE_CHANNEL_SETUP, receivedMessage.getJMSCorrelationID(), true,
                    m -> m.setJMSReplyTo(uploadQueue)));
    return verifier;
  }

  private void eos(Message msg, boolean v16) throws Exception {
    sendTo(msg.getJMSReplyTo(), textMsg("eos", MESSAGE_TYPE_STREAM_CLOSED, msg.getJMSCorrelationID(), v16));
  }

  private void keepalive(Message msg, long until, boolean v16) throws Exception {
    sendTo(msg.getJMSReplyTo(), textMsg("keepalive", MESSAGE_TYPE_EXTEND_WAIT, msg.getJMSCorrelationID(), v16,
            m -> m.setLongProperty(PROPERTY_REQ_TIMEOUT, until)));
  }

  private void reply(Message msg, Serializable obj, boolean v16) throws Exception {
    if (v16) {
      sendTo(msg.getJMSReplyTo(), byteMsg(obj, MESSAGE_TYPE_SIGNAL_RESPONSE, msg.getJMSCorrelationID()));
    } else {
      sendTo(msg.getJMSReplyTo(), objMsg(obj, MESSAGE_TYPE_SIGNAL_RESPONSE, msg.getJMSCorrelationID()));
    }
  }

  private void sendTo(Destination destination, Message msg) throws NamingException, JMSException {
    MessageProducer producer = session.createProducer(destination);
    producer.send(msg);
    producer.close();
  }

  private class ChannelUploadVerifier {
    final Object expectedObject;

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final MessageDigest digest = JMSUtils.md5();
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
      assertEquals(expectedObject, JMSUtils.unserialize(baos.toByteArray()));
    }

    void receive(Message msg) {
      try {
        assertFalse(eos.isDone());
        String messageType = msg.getStringProperty(PROPERTY_MESSAGE_TYPE);
        //expect stream to terminate with an EOS message
        if (messageType.equals(MESSAGE_TYPE_STREAM_CLOSED)) {
          baos.close();
          //make sure fragment count and checksum are correct
          assertEquals(msg.getIntProperty(PROPERTY_FRAGMENTS_TOTAL), fragmentCount);
          String checksum = msg.getStringProperty(PROPERTY_DATA_CHECKSUM_MD5);
          assertEquals(checksum, JMSUtils.hex(digest.digest()));
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
}
