package no.mnemonic.messaging.requestsink;

import no.mnemonic.commons.utilities.collections.SetUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
public class RequestHandlerTest {

  private static final ExecutorService executor = Executors.newSingleThreadExecutor();
  @Mock
  private RequestListener requestListener;
  @Mock
  private ResponseListener responseListener;

  @AfterAll
  public static void afterAll() {
    executor.shutdown();
  }

  @Test
  public void testCloseHandler() {
    RequestHandler handler = createHandler();
    handler.addListener(requestListener);
    assertFalse(handler.isClosed());
    handler.close();
    assertTrue(handler.isClosed());
    verify(requestListener).close("callid");
  }

  @Test
  public void testGetResponsesNoWaitWithoutResults() throws InvocationTargetException {
    RequestHandler handler = RequestHandler.builder().setAllowKeepAlive(false).setCallID("callid").setMaxWait(10000).build();
    assertTrue(handler.addResponse(new TestMessage("msg"), responseListener));
    verifyNoInteractions(responseListener);
    assertNotNull(handler.getNextResponse());
    verify(responseListener).responseAccepted();
  }

  @Test
  public void testGetResponsesNoWaitWithResult() throws InvocationTargetException {
    RequestHandler handler = createHandler();
    assertTrue(handler.addResponse(new TestMessage("msg"), responseListener));
    Collection<TestMessage> response = handler.getResponsesNoWait();
    assertEquals(1, response.size());
    assertEquals("msg", response.iterator().next().getMsgID());
  }

  @Test
  public void testGetNextResponseSendsAck() throws InvocationTargetException {
    RequestHandler handler = createHandler();
    assertTrue(handler.addResponse(new TestMessage("msg"), responseListener));
    Collection<TestMessage> response = handler.getResponsesNoWait();
    assertEquals(1, response.size());
    assertEquals("msg", response.iterator().next().getMsgID());
  }

  @Test
  public void testGetResponsesNoWaitWithMultipleResults() throws InvocationTargetException {
    RequestHandler handler = createHandler();
    assertTrue(handler.addResponse(new TestMessage("msg1"), responseListener));
    assertTrue(handler.addResponse(new TestMessage("msg2"), responseListener));
    assertTrue(handler.addResponse(new TestMessage("msg3"), responseListener));
    assertEquals(SetUtils.set("msg1", "msg2", "msg3"), SetUtils.set(handler.getResponsesNoWait(), m -> ((TestMessage) m).getMsgID()));
  }

  @Test
  public void testGetResponsesNoWaitEnqueuesMoreResults() throws InvocationTargetException {
    RequestHandler handler = createHandler();
    assertTrue(handler.addResponse(new TestMessage("msg1"), responseListener));
    assertTrue(handler.addResponse(new TestMessage("msg2"), responseListener));
    assertEquals(SetUtils.set("msg1", "msg2"), SetUtils.set(handler.getResponsesNoWait(), m -> ((TestMessage) m).getMsgID()));
    assertTrue(handler.addResponse(new TestMessage("msg3"), responseListener));
    assertEquals(SetUtils.set("msg3"), SetUtils.set(handler.getResponsesNoWait(), m -> ((TestMessage) m).getMsgID()));
  }

  @Test
  public void testGetResponsesNoWaitWithError() {
    RequestHandler handler = createHandler();
    assertTrue(handler.addResponse(new TestMessage("msg"), responseListener));
    handler.notifyError(new IllegalArgumentException("invalid"));
    assertThrows(InvocationTargetException.class, handler::getResponsesNoWait);
  }

  @Test
  public void testGetNextResponse() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = createHandler();
    Future<TestMessage> msg = executor.submit(() -> handler.getNextResponse(1000));
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg"), responseListener);
    assertNotNull(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetResponsesWaitForTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = createHandler();
    Future<Collection<TestMessage>> msg = executor.submit(() -> handler.getResponses(200, 3));
    handler.addResponse(new TestMessage("msg1"), responseListener);
    assertEquals(1, msg.get(500, TimeUnit.MILLISECONDS).size());
  }

  @Test
  public void testGetResponsesWaitForEndOfStream() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = RequestHandler.builder()
        .setAllowKeepAlive(true)
        .setCallID("callid")
        .setMaxWait(10000)
        .build();
    Future<TestMessage> msg = executor.submit((Callable<TestMessage>) handler::getNextResponse);
    Thread.sleep(300);
    assertFalse(msg.isDone());
    handler.endOfStream();
    assertNull(msg.get(500, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetResponsesWaitForMessage() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = RequestHandler.builder()
        .setAllowKeepAlive(true)
        .setCallID("callid")
        .setMaxWait(10000)
        .build();
    Future<TestMessage> msg = executor.submit((Callable<TestMessage>) handler::getNextResponse);
    Thread.sleep(300);
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg1"), responseListener);
    assertEquals("msg1", msg.get(500, TimeUnit.MILLISECONDS).getMsgID());
  }

  @Test
  public void testGetResponsesReturnError() throws Throwable {
    RequestHandler handler = createHandler();
    Future<Collection<TestMessage>> msg = executor.submit(() -> handler.getResponses(200, 3));
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg1"), responseListener);
    Thread.sleep(100);
    handler.notifyError(new IllegalArgumentException("invalid"));
    ExecutionException ex = assertThrows(ExecutionException.class, () -> msg.get(1000, TimeUnit.MILLISECONDS));
    assertTrue(ex.getCause() instanceof InvocationTargetException);
  }

  @Test
  public void testGetResponsesReturnsPendingResultsWhenClosed() throws Throwable {
    RequestHandler handler = createHandler();
    handler.addResponse(new TestMessage("msg3"), responseListener);
    handler.endOfStream();
    assertNotNull(executor.submit(() -> handler.getNextResponse(100)).get(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetResponsesReturnsNullWhenClosedAndNoMoreResults() throws Throwable {
    RequestHandler handler = createHandler();
    handler.addResponse(new TestMessage("msg3"), responseListener);
    handler.endOfStream();
    assertNotNull(executor.submit(() -> handler.getNextResponse(100)).get(100, TimeUnit.MILLISECONDS));
    assertNull(executor.submit(() -> handler.getNextResponse(100)).get(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetResponsesWaitForResults() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = createHandler();
    Future<Collection<TestMessage>> msg = executor.submit(() -> handler.getResponses(1000, 3));
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg1"), responseListener);
    Thread.sleep(100);
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg2"), responseListener);
    Thread.sleep(100);
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg3"), responseListener);
    assertEquals(3, msg.get(100, TimeUnit.MILLISECONDS).size());
  }

  @Test
  public void testGetNextResponseWhenError() {
    RequestHandler handler = createHandler();
    Future<TestMessage> msg = executor.submit(() -> handler.getNextResponse(1000));
    assertFalse(msg.isDone());
    handler.notifyError(new IllegalArgumentException("invalid"));
    ExecutionException ex = assertThrows(ExecutionException.class, () -> msg.get(1000, TimeUnit.MILLISECONDS));
    assertTrue(ex.getCause() instanceof InvocationTargetException);
  }

  @Test
  public void testGetNextResponseReturnsWhenEOS() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = createHandler();
    Future<TestMessage> msg = executor.submit(() -> handler.getNextResponse(1000));
    assertFalse(msg.isDone());
    handler.endOfStream();
    assertNull(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetNextResponseTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = createHandler();
    Future<TestMessage> msg = executor.submit(() -> handler.getNextResponse(100));
    assertFalse(msg.isDone());
    assertNull(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWaitForEndOfStream() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = createHandler();
    Future<Boolean> msg = executor.submit(() -> handler.waitForEndOfStream(1000));
    assertFalse(msg.isDone());
    handler.endOfStream();
    assertTrue(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWaitForEndOfStreamTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = RequestHandler.builder()
        .setAllowKeepAlive(false)
        .setCallID("callid")
        .setMaxWait(100)
        .build();
    Future<Boolean> msg = executor.submit(() -> handler.waitForEndOfStream(150));
    assertFalse(msg.isDone());
    assertTrue(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWaitForEndOfStreamKeepAliveNotEnabled() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = RequestHandler.builder()
        .setAllowKeepAlive(false)
        .setCallID("callid")
        .setMaxWait(500)
        .build();
    //wait for end of stream, wait at most 100ms before closing
    Future<Boolean> msg = executor.submit(() -> handler.waitForEndOfStream(600));
    assertFalse(msg.isDone());
    //send a keepalive to handler, which should be ignored (allowKeepAlive is false)
    assertFalse(handler.keepAlive(System.currentTimeMillis() + 1000));
    //when waitForEndOfStream resolves, it should return true (channel was closed anyway)
    assertTrue(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWaitForEndOfStreamKeepAlive() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = RequestHandler.builder()
        .setAllowKeepAlive(false)
        .setCallID("callid")
        .setMaxWait(200)
        .build();
    //wait for end of stream, wait at most 100ms before closing
    Future<Boolean> msg = executor.submit(() -> handler.waitForEndOfStream(100));
    assertFalse(msg.isDone());
    //when waitForEndOfStream resolves, it should return false (channel was kept open due to keepalive)
    assertFalse(msg.get(1000, TimeUnit.MILLISECONDS));
    //wait more for end of stream, wait at most 200ms before closing
    msg = executor.submit(() -> handler.waitForEndOfStream(200));
    //when waitForEndOfStream resolves, it should return true (channel was closed)
    assertTrue(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  private static RequestHandler createHandler() {
    return RequestHandler.builder()
        .setAllowKeepAlive(false)
        .setCallID("callid")
        .setMaxWait(10000)
        .build();
  }

  public static class TestMessage implements Message {
    private final String msgID;
    private String callID = "callid";
    private final long messageTimestamp = System.currentTimeMillis();

    TestMessage(String msgID) {
      this.msgID = msgID;
    }

    public String getMsgID() {
      return msgID;
    }

    @Override
    public String getCallID() {
      return callID;
    }


    @Override
    public long getMessageTimestamp() {
      return messageTimestamp;
    }
  }
}
