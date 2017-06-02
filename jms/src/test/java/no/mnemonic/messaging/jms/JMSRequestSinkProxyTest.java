package no.mnemonic.messaging.jms;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.api.Message;
import no.mnemonic.messaging.api.RequestSink;
import no.mnemonic.messaging.api.SignalContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

public class JMSRequestSinkProxyTest extends AbstractJMSRequestTest {

  private JMSRequestSink requestSink;
  private JMSRequestProxy requestProxy;
  private ComponentContainer container;
  private RequestSink endpoint;
  private SignalContext signalContext;
  private JMSConnection connection;
  private String queueName;

  @Before
  public void setUp() throws Exception {

    //create mock client (requestor to requestSink) and endpoint (target for requestProxy)
    endpoint = mock(RequestSink.class);
    signalContext = mock(SignalContext.class);

    //set up a real JMS connection to a vm-local activemq
    connection = createConnection();
    queueName = "dynamicQueues/" + generateCookie(10);

    //set up request sink pointing at a vm-local topic
    requestSink = JMSRequestSink.builder()
            .addConnection(connection)
            .setDestinationName(queueName)
            .build();

    //set up request proxy listening to the vm-local topic, and pointing to mock endpoint
    requestProxy = JMSRequestProxy.builder()
            .addConnection(connection)
            .setDestinationName(queueName)
            .setRequestSink(endpoint)
            .build();

    container = ComponentContainer.create(requestSink, requestProxy, connection);
  }

  @After
  public void tearDown() throws Exception {
    container.destroy();
  }

  @Test
  public void testSignal() throws Exception {
    container.initialize();
    //mock client handling of response
    Future<List<TestMessage>> response = mockReceiveResponse();
    //when endpoint receives signal, it replies with a single reply, before closing channel
    Future<TestMessage> signal = mockEndpointSignal(new TestMessage("reply"));
    //whenever SignalContext.isClosed() is called, return the state of the AtomicBoolean
    when(signalContext.isClosed()).thenAnswer(i -> response.isDone());

    requestSink.signal(new TestMessage("request"), signalContext, 10000);
    //wait for reply
    assertEquals(1, response.get(1000, TimeUnit.MILLISECONDS).size());
    //verify that client was given resultsby request sink, and that context was closed
    verify(signalContext, times(1)).addResponse(any());
  }

  @Test
  public void testInvalidation() throws InterruptedException {
    container.initialize();
    requestProxy.invalidate();
    assertFalse(requestProxy.hasSession());
    Thread.sleep(1500);
    //requestProxy.setDestinationName("dynamicTopics/testtopic");
    Thread.sleep(1500);
    assertTrue(requestProxy.hasSession());
  }

  @Test
  public void testChannelUpload() throws InterruptedException, TimeoutException, ExecutionException {
    //set up request sink pointing at a vm-local topic
    requestSink = JMSRequestSink.builder()
            .addConnection(connection)
            .setDestinationName(queueName)
            //set protocol V16 to enable channel upload
            .setProtocolVersion(ProtocolVersion.V16)
            //set max message size to 100 bytes, to force channel upload with message fragments
            .setMaxMessageSize(100)
            .build();
    container = ComponentContainer.create(requestSink, requestProxy, connection);
    container.initialize();

    //send message bigger than max message size
    TestMessage msg = new TestMessage(generateCookie(1000));
    TestMessage reply = new TestMessage("reply");

    Future<TestMessage> signal = mockEndpointSignal(reply);
    Future<List<TestMessage>> response = mockReceiveResponse();

    requestSink.signal(msg, signalContext, 10000);
    assertEquals(msg, signal.get(1000, TimeUnit.MILLISECONDS));
    assertEquals(1, response.get(1000, TimeUnit.MILLISECONDS).size());
    assertEquals(reply, response.get().get(0));
  }

  @Test
  public void testSignalMultiReplies() throws InterruptedException, TimeoutException, ExecutionException {
    container.initialize();
    //when endpoint receives signal, it replies with three replies, before closing channel
    Future<TestMessage> signal = mockEndpointSignal(new TestMessage("reply1"), new TestMessage("reply2"), new TestMessage("reply3"));
    //mock client handling of response
    Future<List<TestMessage>> response = mockReceiveResponse();
    //whenever SignalContext.isClosed() is called, return the state of the AtomicBoolean
    when(signalContext.isClosed()).thenAnswer(i -> response.isDone());

    //do request
    requestSink.signal(new TestMessage("request"), signalContext, 10000);
    //wait for replies
    assertEquals(3, response.get(1000, TimeUnit.MILLISECONDS).size());
    //verify that client was given resultsby request sink, and that context was closed
    verify(signalContext, times(3)).addResponse(isA(Message.class));
  }

  @Test
  public void testSignalSequence() throws InterruptedException, TimeoutException, ExecutionException {
    container.initialize();
    Semaphore sem = new Semaphore(0);
    doAnswer(i -> {
      sem.release();
      return null;
    }).when(signalContext).endOfStream();
    when(endpoint.signal(isA(TestMessage.class), isA(SignalContext.class), anyLong())).thenAnswer(i -> {
      SignalContext ctx = (SignalContext) i.getArguments()[1];
      ctx.addResponse(new TestMessage("reploy"));
      ctx.endOfStream();
      return ctx;
    });

    //do first request
    requestSink.signal(new TestMessage("request"), signalContext, 10000);
    //verify that reply was received and context closed
    assertTrue(sem.tryAcquire(1000, TimeUnit.MILLISECONDS));
    verify(signalContext, times(1)).addResponse(isA(Message.class));
    verify(endpoint, times(1)).signal(any(), any(), anyLong());

    //do second request
    requestSink.signal(new TestMessage("request"), signalContext, 10000);
    //verify that reply was received and context closed
    assertTrue(sem.tryAcquire(1000, TimeUnit.MILLISECONDS));
    verify(signalContext, times(2)).addResponse(isA(Message.class));
    verify(endpoint, times(2)).signal(any(), any(), anyLong());
  }

  //helpers

  private Future<TestMessage> mockEndpointSignal(TestMessage... replies) {
    CompletableFuture<TestMessage> signal = new CompletableFuture<>();
    when(endpoint.signal(isA(TestMessage.class), isA(SignalContext.class), anyLong())).thenAnswer(i -> {
      TestMessage msg = i.getArgument(0);
      SignalContext ctx = (SignalContext) i.getArguments()[1];
      System.out.println(String.format("Received request, responding with %d replies", replies.length));
      for (TestMessage reply : replies) {
        ctx.addResponse(reply);
      }
      System.out.println("Closing");
      ctx.endOfStream();
      signal.complete(msg);
      return ctx;
    });
    return signal;
  }

  private Future<List<TestMessage>> mockReceiveResponse() {
    List<TestMessage> responses = new ArrayList<>();
    CompletableFuture<List<TestMessage>> endOfStream = new CompletableFuture<>();
    when(signalContext.addResponse(isA(TestMessage.class))).thenAnswer(i -> {
      if (endOfStream.isDone()) throw new IllegalStateException("Received response to closed client");
      responses.add(i.getArgument(0));
      return true;
    });
    doAnswer(i -> endOfStream.complete(responses)).when(signalContext).endOfStream();
    return endOfStream;
  }

}
