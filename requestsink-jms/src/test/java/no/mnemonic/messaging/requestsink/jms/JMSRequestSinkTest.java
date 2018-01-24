package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.requestsink.RequestContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class JMSRequestSinkTest extends AbstractJMSRequestTest {

  @Mock
  private RequestContext requestContext;

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
  public void testSignalSubmitsMessage() throws Exception {
    setupSinkAndContainer(65536, ProtocolVersion.V2);
    TestMessage testMessage = new TestMessage("test1");
    //send testmessage
    requestSink.signal(testMessage, requestContext, 10000);
    //wait for message to come through and validate
    Message receivedMessage = expectMessage(JMSRequestProxy.MESSAGE_TYPE_SIGNAL);
    Assert.assertEquals(ProtocolVersion.V2.getVersionString(), receivedMessage.getStringProperty(JMSBase.PROTOCOL_VERSION_KEY));
    Assert.assertEquals(JMSRequestProxy.MESSAGE_TYPE_SIGNAL, receivedMessage.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    assertEquals(testMessage, JMSUtils.extractObject(receivedMessage));
    assertTrue(receivedMessage instanceof BytesMessage);
  }

  @Test
  public void testSignalWithProtocolLevelV1() throws Exception {
    setupSinkAndContainer(65536, ProtocolVersion.V1);
    TestMessage testMessage = new TestMessage("test1");
    //send testmessage
    requestSink.signal(testMessage, requestContext, 10000);
    //wait for message to come through and validate
    Message receivedMessage = expectMessage(JMSRequestProxy.MESSAGE_TYPE_SIGNAL);
    Assert.assertEquals(ProtocolVersion.V1.getVersionString(), receivedMessage.getStringProperty(JMSBase.PROTOCOL_VERSION_KEY));
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
    setupSinkAndContainer(65536, ProtocolVersion.V2);
    TestMessage testMessage = new TestMessage("test1");
    TestMessage responseMessage = new TestMessage("huge response");

    //signal message
    requestSink.signal(testMessage, requestContext, 10000);
    //wait for message to come through
    Message receivedMessage = expectMessage(JMSRequestProxy.MESSAGE_TYPE_SIGNAL);
    UUID responseID = UUID.randomUUID();

    //fragment response message into fragments of 3 bytes
    JMSUtils.fragment(new ByteArrayInputStream(JMSUtils.serialize(responseMessage)), 3, new JMSUtils.FragmentConsumer() {
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
      public void end(int fragments, byte[] digest) throws JMSException {
        try {
          //end-of-fragment message contains responseID, fragment count and md5 checksum
          sendTo(
                  receivedMessage.getJMSReplyTo(),
                  textMsg("end-of-fragments", JMSRequestProxy.MESSAGE_TYPE_END_OF_FRAGMENTED_MESSAGE, receivedMessage.getJMSCorrelationID(),
                          msg -> msg.setStringProperty(JMSRequestProxy.PROPERTY_RESPONSE_ID, responseID.toString()),
                          msg -> msg.setIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_TOTAL, fragments),
                          msg -> msg.setStringProperty(JMSRequestProxy.PROPERTY_DATA_CHECKSUM_MD5, JMSUtils.hex(digest))
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
    verify(requestContext).addResponse(eq(responseMessage));
  }


  @Test
  public void testSignalKeepalive() throws Exception {
    setupSinkAndContainer(65536, ProtocolVersion.V2);
    TestMessage testMessage = new TestMessage("test1");
    //signal message with short lifetime
    requestSink.signal(testMessage, requestContext, 1000);
    //wait for message to come through
    Message receivedMessage = expectMessage(JMSRequestProxy.MESSAGE_TYPE_SIGNAL);
    keepalive(receivedMessage, 1000);
    eos(receivedMessage);
    waitForEOS();
    verify(requestContext).keepAlive(1000);
  }

  @Test
  public void testChannelUploadWithResponse() throws Exception {
    setupSinkAndContainer(100, ProtocolVersion.V2);

    TestMessage testMessage = new TestMessage(generateCookie(500));
    requestSink.signal(testMessage, requestContext, 1000);

    //expect channel request
    Message receivedMessage = expectMessage(JMSRequestProxy.MESSAGE_TYPE_CHANNEL_REQUEST);
    ChannelUploadVerifier verifier = setupChannel(receivedMessage, testMessage);
    verifier.waitForEOS();
    verifier.verify();

    //when channel has uploaded, send response
    reply(receivedMessage, new TestMessage("response"));
    eos(receivedMessage);

    waitForEOS();
    verify(requestContext).addResponse(eq(new TestMessage("response")));
    verify(requestContext).endOfStream();
  }

  //helper methods

  private void doTestSignalReceiveResults(int resultCount) throws Exception {
    setupSinkAndContainer(65536, ProtocolVersion.V2);
    TestMessage testMessage = new TestMessage("test1");
    TestMessage responseMessage = new TestMessage("response");

    //signal message
    requestSink.signal(testMessage, requestContext, 10000);
    //wait for message to come through
    Message receivedMessage = expectMessage(JMSRequestProxy.MESSAGE_TYPE_SIGNAL);
    //send reply and EOS
    for (int i = 0; i < resultCount; i++) {
      reply(receivedMessage, responseMessage);
    }
    eos(receivedMessage);
    waitForEOS();
    //verify that the response got through as well
    verify(requestContext, times(resultCount)).addResponse(eq(responseMessage));
  }

  private void setupSinkAndContainer(int maxMessageSize, ProtocolVersion protocolVersion) {
    requestSink = JMSRequestSink.builder()
            .addConnection(connection)
            .setDestinationName(queueName)
            .setProtocolVersion(protocolVersion)
            .setMaxMessageSize(maxMessageSize)
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
    assertEquals(messageType, receivedMessage.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE));
    return receivedMessage;
  }

  private Future<Void> expectEndOfStream() {
    CompletableFuture<Void> eos = new CompletableFuture<>();
    doAnswer(i -> eos.complete(null)).when(requestContext).endOfStream();
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

  private void reply(Message msg, Serializable obj) throws Exception {
    sendTo(msg.getJMSReplyTo(), byteMsg(obj, JMSRequestProxy.MESSAGE_TYPE_SIGNAL_RESPONSE, msg.getJMSCorrelationID()));
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
        String messageType = msg.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE);
        //expect stream to terminate with an EOS message
        if (messageType.equals(JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED)) {
          baos.close();
          //make sure fragment count and checksum are correct
          assertEquals(msg.getIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_TOTAL), fragmentCount);
          String checksum = msg.getStringProperty(JMSRequestProxy.PROPERTY_DATA_CHECKSUM_MD5);
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
