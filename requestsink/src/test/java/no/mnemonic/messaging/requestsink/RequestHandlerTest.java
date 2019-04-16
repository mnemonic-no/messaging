package no.mnemonic.messaging.requestsink;

import no.mnemonic.commons.utilities.collections.SetUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class RequestHandlerTest {

  private static ExecutorService executor = Executors.newSingleThreadExecutor();

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @AfterClass
  public static void afterAll() {
    executor.shutdown();
  }

  @Test
  public void testCloseHandler() {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    assertFalse(handler.isClosed());
    handler.close();
    assertTrue(handler.isClosed());
  }

  @Test
  public void testGetResponsesNoWaitWithoutResults() throws InvocationTargetException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    assertTrue(handler.getResponsesNoWait().isEmpty());
  }

  @Test
  public void testGetResponsesNoWaitWithResult() throws InvocationTargetException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    assertTrue(handler.addResponse(new TestMessage("msg")));
    Collection<TestMessage> response = handler.getResponsesNoWait();
    assertEquals(1, response.size());
    assertEquals("msg", response.iterator().next().getMsgID());
  }

  @Test
  public void testGetResponsesNoWaitWithMultipleResults() throws InvocationTargetException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    assertTrue(handler.addResponse(new TestMessage("msg1")));
    assertTrue(handler.addResponse(new TestMessage("msg2")));
    assertTrue(handler.addResponse(new TestMessage("msg3")));
    assertEquals(SetUtils.set("msg1", "msg2", "msg3"), SetUtils.set(handler.getResponsesNoWait(), m -> ((TestMessage) m).getMsgID()));
  }

  @Test
  public void testGetResponsesNoWaitEnqueuesMoreResults() throws InvocationTargetException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    assertTrue(handler.addResponse(new TestMessage("msg1")));
    assertTrue(handler.addResponse(new TestMessage("msg2")));
    assertEquals(SetUtils.set("msg1", "msg2"), SetUtils.set(handler.getResponsesNoWait(), m -> ((TestMessage) m).getMsgID()));
    assertTrue(handler.addResponse(new TestMessage("msg3")));
    assertEquals(SetUtils.set("msg3"), SetUtils.set(handler.getResponsesNoWait(), m -> ((TestMessage) m).getMsgID()));
  }

  @Test(expected = InvocationTargetException.class)
  public void testGetResponsesNoWaitWithError() throws InvocationTargetException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    assertTrue(handler.addResponse(new TestMessage("msg")));
    handler.notifyError(new IllegalArgumentException("invalid"));
    handler.getResponsesNoWait();
  }

  @Test
  public void testGetNextResponse() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    Future<TestMessage> msg = executor.submit(() -> handler.getNextResponse(1000));
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg"));
    assertNotNull(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetResponsesWaitForTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    Future<Collection<TestMessage>> msg = executor.submit(() -> handler.getResponses(200, 3));
    handler.addResponse(new TestMessage("msg1"));
    assertEquals(1, msg.get(500, TimeUnit.MILLISECONDS).size());
  }

  @Test
  public void testGetResponsesWaitForEndOfStream() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(true, "callid", 10000);
    Future<TestMessage> msg = executor.submit((Callable<TestMessage>) handler::getNextResponse);
    Thread.sleep(300);
    assertFalse(msg.isDone());
    handler.endOfStream();
    assertNull(msg.get(500, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetResponsesWaitForMessage() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(true, "callid", 10000);
    Future<TestMessage> msg = executor.submit((Callable<TestMessage>) handler::getNextResponse);
    Thread.sleep(300);
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg1"));
    assertEquals("msg1", msg.get(500, TimeUnit.MILLISECONDS).getMsgID());
  }

  @Test(expected = InvocationTargetException.class)
  public void testGetResponsesReturnError() throws Throwable {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    Future<Collection<TestMessage>> msg = executor.submit(() -> handler.getResponses(200, 3));
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg1"));
    Thread.sleep(100);
    handler.notifyError(new IllegalArgumentException("invalid"));
    try {
      msg.get(1000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testGetResponsesReturnsPendingResultsWhenClosed() throws Throwable {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    handler.addResponse(new TestMessage("msg3"));
    handler.endOfStream();
    assertNotNull(executor.submit(() -> handler.getNextResponse(100)).get(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetResponsesReturnsNullWhenClosedAndNoMoreResults() throws Throwable {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    handler.addResponse(new TestMessage("msg3"));
    handler.endOfStream();
    assertNotNull(executor.submit(() -> handler.getNextResponse(100)).get(100, TimeUnit.MILLISECONDS));
    assertNull(executor.submit(() -> handler.getNextResponse(100)).get(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetResponsesWaitForResults() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    Future<Collection<TestMessage>> msg = executor.submit(() -> handler.getResponses(1000, 3));
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg1"));
    Thread.sleep(100);
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg2"));
    Thread.sleep(100);
    assertFalse(msg.isDone());
    handler.addResponse(new TestMessage("msg3"));
    assertEquals(3, msg.get(100, TimeUnit.MILLISECONDS).size());
  }

  @Test(expected = InvocationTargetException.class)
  public void testGetNextResponseWhenError() throws Throwable {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    Future<TestMessage> msg = executor.submit(() -> handler.getNextResponse(1000));
    assertFalse(msg.isDone());
    handler.notifyError(new IllegalArgumentException("invalid"));
    try {
      msg.get(1000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testGetNextResponseReturnsWhenEOS() throws InvocationTargetException, InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    Future<TestMessage> msg = executor.submit(() -> handler.getNextResponse(1000));
    assertFalse(msg.isDone());
    handler.endOfStream();
    assertNull(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetNextResponseTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    Future<TestMessage> msg = executor.submit(() -> handler.getNextResponse(100));
    assertFalse(msg.isDone());
    assertNull(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWaitForEndOfStream() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(false, "callid", 10000);
    Future<Boolean> msg = executor.submit(() -> handler.waitForEndOfStream(1000));
    assertFalse(msg.isDone());
    handler.endOfStream();
    assertTrue(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWaitForEndOfStreamTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(false, "callid", 100);
    Future<Boolean> msg = executor.submit(() -> handler.waitForEndOfStream(150));
    assertFalse(msg.isDone());
    assertTrue(msg.get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWaitForEndOfStreamKeepAliveNotEnabled() throws InterruptedException, ExecutionException, TimeoutException {
    RequestHandler handler = new RequestHandler(false, "callid", 500);
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
    RequestHandler handler = new RequestHandler(true, "callid", 200);
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
