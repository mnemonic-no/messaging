package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.messaging.requestsink.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClientRequestStateTest {

  @Mock
  private RequestContext requestContext;
  private ClientRequestState state;

  private TestMessage message = new TestMessage("abc");
  private byte[] messageBytes;

  @Before
  public void setup() throws JMSException, IOException {
    MockitoAnnotations.initMocks(this);
    when(requestContext.addResponse(any())).thenReturn(true);
    state = new ClientRequestState(requestContext, ClassLoader.getSystemClassLoader());
    messageBytes = JMSUtils.serialize(message);
  }

  @Test
  public void testHandleFragments() throws IOException {
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    assertTrue(state.reassembleFragments("responseID", 2, JMSUtils.md5(messageBytes)));
    verify(requestContext).addResponse(eq(message));
  }

  @Test
  public void testMultipleFragmentedResponses() throws IOException {
    TestMessage message1 = new TestMessage("abc");
    TestMessage message2 = new TestMessage("def");
    byte[] messageBytes1 = JMSUtils.serialize(message1);
    byte[] messageBytes2 = JMSUtils.serialize(message2);

    assertTrue(state.addFragment(new MessageFragment("callID", "responseID1", 0, Arrays.copyOfRange(messageBytes1, 0, 3))));
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID1", 1, Arrays.copyOfRange(messageBytes1, 3, messageBytes1.length))));
    assertTrue(state.reassembleFragments("responseID1", 2, JMSUtils.md5(messageBytes1)));

    assertTrue(state.addFragment(new MessageFragment("callID", "responseID2", 0, Arrays.copyOfRange(messageBytes2, 0, 3))));
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID2", 1, Arrays.copyOfRange(messageBytes2, 3, messageBytes2.length))));
    assertTrue(state.reassembleFragments("responseID2", 2, JMSUtils.md5(messageBytes2)));

    verify(requestContext).addResponse(eq(message1));
    verify(requestContext).addResponse(eq(message2));
  }

  @Test
  public void testMultipleFragmentedResponsesOutOfOrder() throws IOException {
    TestMessage message1 = new TestMessage("abc");
    TestMessage message2 = new TestMessage("def");
    byte[] messageBytes1 = JMSUtils.serialize(message1);
    byte[] messageBytes2 = JMSUtils.serialize(message2);

    assertTrue(state.addFragment(new MessageFragment("callID", "responseID2", 0, Arrays.copyOfRange(messageBytes2, 0, 3))));
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID2", 1, Arrays.copyOfRange(messageBytes2, 3, messageBytes2.length))));
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID1", 0, Arrays.copyOfRange(messageBytes1, 0, 3))));
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID1", 1, Arrays.copyOfRange(messageBytes1, 3, messageBytes1.length))));

    assertTrue(state.reassembleFragments("responseID2", 2, JMSUtils.md5(messageBytes2)));
    assertTrue(state.reassembleFragments("responseID1", 2, JMSUtils.md5(messageBytes1)));

    verify(requestContext).addResponse(eq(message1));
    verify(requestContext).addResponse(eq(message2));
  }

  @Test
  public void testClosedRequestContextRejectsResponse() throws IOException {
    when(requestContext.addResponse(any())).thenReturn(false);
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    assertFalse(state.reassembleFragments("responseID", 2, JMSUtils.md5(messageBytes)));
  }

  @Test
  public void testInvalidChecksumRejectsResponse() throws IOException {
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    assertFalse(state.reassembleFragments("responseID", 2, "invalid"));
  }

  @Test
  public void testInvalidTotalCountRejectsResponse() throws IOException {
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID", 0, Arrays.copyOfRange(messageBytes, 0, 3))));
    assertTrue(state.addFragment(new MessageFragment("callID", "responseID", 1, Arrays.copyOfRange(messageBytes, 3, messageBytes.length))));
    assertFalse(state.reassembleFragments("responseID", 3, "invalid"));
  }

  @Test
  public void testReassembleWithoutFragmentsRejectsResponse() throws IOException {
    assertFalse(state.reassembleFragments("responseID", 2, JMSUtils.md5(messageBytes)));
  }

  @Test
  public void testNullFragment() throws IOException {
    assertFalse(state.addFragment(null));
  }
}
