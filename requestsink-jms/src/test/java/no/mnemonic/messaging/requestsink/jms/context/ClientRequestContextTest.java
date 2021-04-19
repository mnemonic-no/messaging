package no.mnemonic.messaging.requestsink.jms.context;

import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.jms.ExceptionMessage;
import no.mnemonic.messaging.requestsink.jms.MockMessageBuilder;
import no.mnemonic.messaging.requestsink.jms.ProtocolVersion;
import no.mnemonic.messaging.requestsink.jms.TestMessage;
import no.mnemonic.messaging.requestsink.jms.TestUtils;
import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import no.mnemonic.messaging.requestsink.jms.util.ClientMetrics;
import no.mnemonic.messaging.requestsink.jms.util.MessageFragment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;

import static no.mnemonic.messaging.requestsink.jms.AbstractJMSRequestBase.*;
import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.md5;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ClientRequestContextTest {

  private static final String CALL_ID = "callID";

  @Mock
  private RequestContext requestContext;
  @Mock
  private Session session;
  @Mock
  private TemporaryQueue temporaryQueue;
  @Mock
  private MessageConsumer messageConsumer;
  @Mock
  private Runnable closeListener;
  @Mock
  private Clock clock;

  private ClientRequestContext handler;
  private final TestMessage testMessage = new TestMessage(CALL_ID);
  private byte[] messageBytes;

  @BeforeEach
  void setup() throws JMSException, IOException {
    lenient().when(clock.millis()).thenReturn(10000L);
    ClientRequestContext.setClock(clock);
    lenient().when(session.createTemporaryQueue()).thenReturn(temporaryQueue);
    lenient().when(session.createConsumer(any())).thenReturn(messageConsumer);
    lenient().when(requestContext.isClosed()).thenReturn(false);
    lenient().when(requestContext.addResponse(any())).thenReturn(true);
    lenient().when(requestContext.keepAlive(anyLong())).thenReturn(true);
    messageBytes = TestUtils.serialize(testMessage);
    handler = new ClientRequestContext(CALL_ID, session, new ClientMetrics(), ClassLoader.getSystemClassLoader(), requestContext, closeListener, new DefaultJavaMessageSerializer());
  }

  @AfterEach
  void cleanup() {
    ClientRequestContext.setClock(Clock.systemUTC());
  }

  @Test
  void testHandleFragments() {
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    assertTrue(handler.reassemble("responseID", 2, md5(messageBytes)));
    verify(requestContext).addResponse(testMessage);
  }

  @Test
  void testHandleFragmentsOutOfOrder() {
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(handler.reassemble("responseID", 2, md5(messageBytes)));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    verify(requestContext).addResponse(testMessage);
  }

  @Test
  void testHandleFragmentSubmitsKeepalive() {
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    verify(requestContext).keepAlive(20000L);
  }

  @Test
  void testCloseListenerNotifiedOnHandlerCleanup() {
    handler.cleanup();
    verify(closeListener).run();
  }

  @Test
  void testMultipleFragmentedResponses() throws IOException {
    TestMessage message1 = new TestMessage("abc");
    TestMessage message2 = new TestMessage("def");
    byte[] messageBytes1 = TestUtils.serialize(message1);
    byte[] messageBytes2 = TestUtils.serialize(message2);

    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID1", 0, Arrays.copyOfRange(messageBytes1, 0, 3))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID1", 1, Arrays.copyOfRange(messageBytes1, 3, messageBytes1.length))));
    assertTrue(handler.reassemble("responseID1", 2, md5(messageBytes1)));

    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID2", 0, Arrays.copyOfRange(messageBytes2, 0, 3))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID2", 1, Arrays.copyOfRange(messageBytes2, 3, messageBytes2.length))));
    assertTrue(handler.reassemble("responseID2", 2, md5(messageBytes2)));

    verify(requestContext).addResponse(message1);
    verify(requestContext).addResponse(message2);
  }

  @Test
  void testMultipleFragmentedResponsesOutOfOrder() throws IOException {
    TestMessage message1 = new TestMessage("abc");
    TestMessage message2 = new TestMessage("def");
    byte[] messageBytes1 = TestUtils.serialize(message1);
    byte[] messageBytes2 = TestUtils.serialize(message2);

    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID2", 0, Arrays.copyOfRange(messageBytes2, 0, 3))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID2", 1, Arrays.copyOfRange(messageBytes2, 3, messageBytes2.length))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID1", 0, Arrays.copyOfRange(messageBytes1, 0, 3))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID1", 1, Arrays.copyOfRange(messageBytes1, 3, messageBytes1.length))));

    assertTrue(handler.reassemble("responseID2", 2, md5(messageBytes2)));
    assertTrue(handler.reassemble("responseID1", 2, md5(messageBytes1)));

    verify(requestContext).addResponse(message1);
    verify(requestContext).addResponse(message2);
  }

  @Test
  void testClosedRequestContextRejectsResponse() {
    when(requestContext.addResponse(any())).thenReturn(false);
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    assertFalse(handler.reassemble("responseID", 2, md5(messageBytes)));
  }

  @Test
  void testInvalidChecksumRejectsResponse() {
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    assertFalse(handler.reassemble("responseID", 2, "invalid"));
  }

  @Test
  void testTooManyFragmentsRejectsResponse() {
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    assertFalse(handler.reassemble("responseID", 1, "invalid"));
  }

  @Test
  void testTooFewFragmentsPendingOutOfOrder() {
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(handler.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    assertTrue(handler.reassemble("responseID", 3, "incomplete"));
    //nothing happens here, as the handler will wait to see if more fragments arrive
  }

  @Test
  void testReassembleWithoutFragmentsRejectsResponse() {
    assertFalse(handler.reassemble("responseID", 2, md5(messageBytes)));
  }

  @Test
  void testNullFragment() {
    assertFalse(handler.addFragment(null));
  }

  @Test
  void testSetupDoesNoInvocations() {
    verifyNoMoreInteractions(session);
  }

  @Test
  void testNullMessage() throws JMSException {
    handler.handleResponse(null);
    verifyNoMoreInteractions(requestContext);
  }

  @Test
  void testMessageWithoutProtocolVersion() throws JMSException {
    handler.handleResponse(new MockMessageBuilder<>(TextMessage.class).build());
    verifyNoMoreInteractions(requestContext);
  }

  @Test
  void testMessageWithoutType() throws JMSException {
    handler.handleResponse(textMessage().build());
    verifyNoMoreInteractions(requestContext);
  }

  @Test
  void testAddSingleResponse() throws JMSException, IOException {
    handler.handleResponse(createResponseMessage(CALL_ID, testMessage));
    verify(requestContext).addResponse(testMessage);
  }

  @Test
  void testAddSingleResponseWithWrongCallID() throws JMSException, IOException {
    handler.handleResponse(createResponseMessage("invalid", testMessage));
    verifyNoMoreInteractions(requestContext);
  }

  @Test
  void testAddSingleResponseWithClosedRequestContext() throws JMSException, IOException {
    when(requestContext.isClosed()).thenReturn(true);
    handler.handleResponse(createResponseMessage(CALL_ID, testMessage));
    verify(requestContext, never()).addResponse(any());
  }

  @Test
  void testEndOfStream() throws JMSException {
    handler.handleResponse(createEOS(CALL_ID));
    verify(requestContext).endOfStream();
  }

  @Test
  void testErrorSignal() throws JMSException, IOException {
    handler.handleResponse(createErrorSignal(CALL_ID, new IllegalStateException()));
    verify(requestContext).notifyError(isA(IllegalStateException.class));
  }

  @Test
  void testExtendWait() throws JMSException {
    handler.handleResponse(createExtendWaitMessage(CALL_ID, 1000));
    verify(requestContext).keepAlive(1000);
  }

  @Test
  void testExtendWaitWithClosedRequestContext() throws JMSException {
    when(requestContext.isClosed()).thenReturn(true);
    handler.handleResponse(createExtendWaitMessage(CALL_ID, 1000));
    verify(requestContext, never()).keepAlive(anyLong());
  }

  @Test
  void testFragmentedResponse() throws JMSException, IOException {
    TestMessage message = new TestMessage("abc");
    byte[] messageBytes = TestUtils.serialize(message);

    handler.handleResponse(createMessageFragment(CALL_ID, "response1", Arrays.copyOfRange(messageBytes, 0, 3), 0));
    handler.handleResponse(createMessageFragment(CALL_ID, "response1", Arrays.copyOfRange(messageBytes, 3, messageBytes.length), 1));
    handler.handleResponse(createEOF(CALL_ID, "response1", 2, md5(messageBytes)));

    verify(requestContext).addResponse(message);
  }

  //helpers

  private BytesMessage createMessageFragment(String callID, String responseID, byte[] data, int idx) throws JMSException {
    return bytesMessage()
            .withData(data)
            .withCorrelationID(callID)
            .withProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_SIGNAL_FRAGMENT)
            .withProperty(PROPERTY_RESPONSE_ID, responseID)
            .withProperty(PROPERTY_FRAGMENTS_IDX, idx)
            .build();
  }

  private TextMessage createEOF(String callID, String responseID, int totalFragments, String checksum) throws JMSException {
    return textMessage()
            .withCorrelationID(callID)
            .withProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_END_OF_FRAGMENTED_MESSAGE)
            .withProperty(PROPERTY_RESPONSE_ID, responseID)
            .withProperty(PROPERTY_FRAGMENTS_TOTAL, totalFragments)
            .withProperty(PROPERTY_DATA_CHECKSUM_MD5, checksum)
            .build();
  }

  private BytesMessage createResponseMessage(String callID, no.mnemonic.messaging.requestsink.Message message) throws IOException, JMSException {
    byte[] data = TestUtils.serialize(message);
    return bytesMessage()
            .withData(data)
            .withCorrelationID(callID)
            .withProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_SIGNAL_RESPONSE)
            .build();
  }

  private Message createExtendWaitMessage(String callID, long timeout) throws JMSException {
    return textMessage()
            .withCorrelationID(callID)
            .withProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_EXTEND_WAIT)
            .withProperty(PROPERTY_REQ_TIMEOUT, timeout)
            .build();
  }

  private Message createErrorSignal(String callID, Throwable error) throws JMSException, IOException {
    return bytesMessage()
            .withCorrelationID(callID)
            .withProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_EXCEPTION)
            .withData(TestUtils.serialize(new ExceptionMessage(callID, error)))
            .build();
  }

  private Message createEOS(String callID) throws JMSException {
    return textMessage()
            .withCorrelationID(callID)
            .withProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_STREAM_CLOSED)
            .build();
  }

  private MockMessageBuilder<TextMessage> textMessage() throws JMSException {
    return new MockMessageBuilder<>(TextMessage.class)
            .withProperty(PROTOCOL_VERSION_KEY, ProtocolVersion.V2.getVersionString());
  }

  private MockMessageBuilder<BytesMessage> bytesMessage() throws JMSException {
    return new MockMessageBuilder<>(BytesMessage.class)
            .withProperty(PROTOCOL_VERSION_KEY, ProtocolVersion.V2.getVersionString());
  }


}
