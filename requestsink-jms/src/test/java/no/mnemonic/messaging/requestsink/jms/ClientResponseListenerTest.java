package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.messaging.requestsink.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.jms.*;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.util.Arrays;

import static no.mnemonic.messaging.requestsink.jms.JMSBase.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.*;

public class ClientResponseListenerTest {

  private static final String CALL_ID = "callID";

  @Mock
  private RequestContext requestContext;
  @Mock
  private Session session;
  @Mock
  private TemporaryQueue temporaryQueue;
  @Mock
  private MessageConsumer messageConsumer;

  private ClientResponseListener listener;
  private ClientRequestState state;
  private TestMessage testMessage = new TestMessage(CALL_ID);

  @Before
  public void setup() throws JMSException {
    MockitoAnnotations.initMocks(this);
    when(session.createTemporaryQueue()).thenReturn(temporaryQueue);
    when(session.createConsumer(any())).thenReturn(messageConsumer);
    when(requestContext.isClosed()).thenReturn(false);
    when(requestContext.addResponse(any())).thenReturn(true);
    when(requestContext.keepAlive(anyLong())).thenReturn(true);
    state = new ClientRequestState(requestContext, ClassLoader.getSystemClassLoader());

    listener = new ClientResponseListener(CALL_ID, session, state, new ClientMetrics());
  }

  @Test
  public void testSetupDoesNoInvocations() throws JMSException {
    verifyNoMoreInteractions(session);
  }

  @Test
  public void testNullMessage() throws JMSException, IOException {
    listener.handleResponse(null);
    verifyNoMoreInteractions(requestContext);
  }

  @Test
  public void testMessageWithoutProtocolVersion() throws JMSException, IOException {
    listener.handleResponse(new MockMessageBuilder<>(TextMessage.class).build());
    verifyNoMoreInteractions(requestContext);
  }

  @Test
  public void testMessageWithoutType() throws JMSException, IOException {
    listener.handleResponse(textMessage().build());
    verifyNoMoreInteractions(requestContext);
  }

  @Test
  public void testAddSingleResponse() throws JMSException, IOException {
    listener.handleResponse(createResponseMessage(CALL_ID, testMessage));
    verify(requestContext).addResponse(eq(testMessage));
  }

  @Test
  public void testAddSingleResponseWithWrongCallID() throws JMSException, IOException {
    listener.handleResponse(createResponseMessage("invalid", testMessage));
    verifyNoMoreInteractions(requestContext);
  }

  @Test
  public void testAddSingleResponseWithClosedRequestContext() throws JMSException, IOException {
    when(requestContext.isClosed()).thenReturn(true);
    listener.handleResponse(createResponseMessage(CALL_ID, testMessage));
    verify(requestContext, never()).addResponse(any());
  }

  @Test
  public void testEndOfStream() throws JMSException {
    listener.handleResponse(createEOS(CALL_ID));
    verify(requestContext).endOfStream();
  }

  @Test
  public void testErrorSignal() throws JMSException, IOException {
    listener.handleResponse(createErrorSignal(CALL_ID, new IllegalStateException()));
    verify(requestContext).notifyError(isA(IllegalStateException.class));
  }

  @Test
  public void testExtendWait() throws JMSException {
    listener.handleResponse(createExtendWaitMessage(CALL_ID, 1000));
    verify(requestContext).keepAlive(1000);
  }

  @Test
  public void testExtendWaitWithClosedRequestContext() throws JMSException {
    when(requestContext.isClosed()).thenReturn(true);
    listener.handleResponse(createExtendWaitMessage(CALL_ID, 1000));
    verify(requestContext, never()).keepAlive(anyLong());
  }

  @Test
  public void testFragmentedResponse() throws JMSException, IOException {
    TestMessage message = new TestMessage("abc");
    byte[] messageBytes = JMSUtils.serialize(message);

    listener.handleResponse(createMessageFragment(CALL_ID, "response1", Arrays.copyOfRange(messageBytes, 0, 3), 0));
    listener.handleResponse(createMessageFragment(CALL_ID, "response1", Arrays.copyOfRange(messageBytes, 3, messageBytes.length), 1));
    listener.handleResponse(createEOF(CALL_ID, "response1", 2, JMSUtils.md5(messageBytes)));

    verify(requestContext).addResponse(eq(message));
  }

  //helpers

  private BytesMessage createMessageFragment(String callID, String responseID, byte[] data, int idx) throws IOException, JMSException {
    return bytesMessage()
            .withData(data)
            .withCorrelationID(callID)
            .withProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_SIGNAL_FRAGMENT)
            .withProperty(PROPERTY_RESPONSE_ID, responseID)
            .withProperty(PROPERTY_FRAGMENTS_IDX, idx)
            .build();
  }

  private TextMessage createEOF(String callID, String responseID, int totalFragments, String checksum) throws IOException, JMSException {
    return textMessage()
            .withCorrelationID(callID)
            .withProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_END_OF_FRAGMENTED_MESSAGE)
            .withProperty(PROPERTY_RESPONSE_ID, responseID)
            .withProperty(PROPERTY_FRAGMENTS_TOTAL, totalFragments)
            .withProperty(PROPERTY_DATA_CHECKSUM_MD5, checksum)
            .build();
  }

  private BytesMessage createResponseMessage(String callID, no.mnemonic.messaging.requestsink.Message message) throws IOException, JMSException {
    byte[] data = JMSUtils.serialize(message);
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
            .withData(JMSUtils.serialize(new ExceptionMessage(callID, error)))
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
