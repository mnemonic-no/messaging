package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.RequestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

public class JMSRequestSinkProxyTest extends AbstractJMSRequestTest {

  private JMSRequestSink requestSink;
  private JMSRequestProxy requestProxy;
  private ComponentContainer clientContainer, serverContainer;
  private RequestSink endpoint;
  private RequestContext requestContext;
  private String queueName;

  private ExecutorService executor = Executors.newCachedThreadPool();

  @Before
  public void setUp() throws Exception {

    //create mock client (requestor to requestSink) and endpoint (target for requestProxy)
    endpoint = mock(RequestSink.class);
    requestContext = mock(RequestContext.class);

    //set up a real JMS connection to a vm-local activemq
    queueName = "dynamicQueues/" + generateCookie(10);

    //set up request sink pointing at a vm-local topic
    requestSink = addConnection(JMSRequestSink.builder())
            .setDestinationName(queueName)
            .build();

    //set up request proxy listening to the vm-local topic, and pointing to mock endpoint
    requestProxy = addConnection(JMSRequestProxy.builder())
            .setMaxMessageSize(1000)
            .setDestinationName(queueName)
            .setRequestSink(endpoint)
            .build();

    clientContainer = ComponentContainer.create(requestSink);
    serverContainer = ComponentContainer.create(requestProxy);
  }

  @After
  public void tearDown() throws Exception {
    clientContainer.destroy();
    serverContainer.destroy();
    executor.shutdown();
  }

  @Test
  public void testSignal() throws Exception {
    serverContainer.initialize();
    clientContainer.initialize();
    //mock client handling of response
    Future<List<TestMessage>> response = mockReceiveResponse();
    //when endpoint receives signal, it replies with a single reply, before closing channel
    mockEndpointSignal(new TestMessage("reply"));
    //whenever SignalContext.isClientClosed() is called, return the state of the AtomicBoolean
    when(requestContext.isClosed()).thenAnswer(i -> response.isDone());

    requestSink.signal(new TestMessage("request"), requestContext, 10000);
    //wait for reply
    assertEquals(1, response.get(1000, TimeUnit.MILLISECONDS).size());
    //verify that client was given resultsby request sink, and that context was closed
    verify(requestContext, times(1)).addResponse(any());
  }

  @Test
  public void testNiceShutdown() throws Exception {
    serverContainer.initialize();
    clientContainer.initialize();

    CompletableFuture<TestMessage> requestReceived = new CompletableFuture<>();
    CompletableFuture<TestMessage> serverResponse = new CompletableFuture<>();
    when(endpoint.signal(isA(TestMessage.class), isA(RequestContext.class), anyLong())).thenAnswer(i -> {
      requestReceived.complete(i.getArgument(0));
      RequestContext ctx = i.getArgument(1);
      ctx.addResponse(serverResponse.get(10000, TimeUnit.MILLISECONDS));
      ctx.endOfStream();
      System.out.println("Finished request");
      return ctx;
    });

    requestSink.signal(new TestMessage("request"), requestContext, 10000);

    //wait for request to be received
    requestReceived.get(1000, TimeUnit.MILLISECONDS);
    //when request is received, start shutting down server
    System.out.println("Shutting down container");
    Future<?> containerShutdown = executor.submit(() -> serverContainer.destroy());

    //make sure server does not shut down (yet) while request is still being processed
    assertFalse(tryTo(() -> containerShutdown.get(2000, TimeUnit.MILLISECONDS)));
    assertFalse(requestProxy.isClosed());

    //now let the server complete the pending request
    System.out.println("Completing request");
    serverResponse.complete(new TestMessage("response"));

    //make sure server shuts down cleanly
    assertTrue(tryTo(() -> containerShutdown.get(5000, TimeUnit.MILLISECONDS)));
    assertTrue(requestProxy.isClosed());
    System.out.println("Verification done");
  }

  @Test
  public void testChannelUpload() throws InterruptedException, TimeoutException, ExecutionException {
    serverContainer.initialize();

    //set up request sink pointing at a vm-local topic
    requestSink = addConnection(JMSRequestSink.builder())
            .setDestinationName(queueName)
            //set protocol V16 to enable channel upload
            .setProtocolVersion(ProtocolVersion.V1)
            //set max message size to 100 bytes, to force channel upload with message fragments
            .setMaxMessageSize(100)
            .build();
    clientContainer = ComponentContainer.create(requestSink);
    clientContainer.initialize();

    //send message bigger than max message size
    TestMessage msg = new TestMessage(generateCookie(1000));
    TestMessage reply = new TestMessage("reply");

    Future<TestMessage> signal = mockEndpointSignal(reply);
    Future<List<TestMessage>> response = mockReceiveResponse();

    requestSink.signal(msg, requestContext, 10000);
    assertEquals(msg, signal.get(1000, TimeUnit.MILLISECONDS));
    assertEquals(1, response.get(1000, TimeUnit.MILLISECONDS).size());
    assertEquals(reply, response.get().get(0));
  }

  @Test
  public void testSignalMultiReplies() throws InterruptedException, TimeoutException, ExecutionException {
    serverContainer.initialize();
    clientContainer.initialize();
    //when endpoint receives signal, it replies with three replies, before closing channel
    mockEndpointSignal(new TestMessage("reply1"), new TestMessage("reply2"), new TestMessage("reply3"));
    //mock client handling of response
    Future<List<TestMessage>> response = mockReceiveResponse();
    //whenever SignalContext.isClientClosed() is called, return the state of the AtomicBoolean
    when(requestContext.isClosed()).thenAnswer(i -> response.isDone());

    //do request
    requestSink.signal(new TestMessage("request"), requestContext, 10000);
    //wait for replies
    assertEquals(3, response.get(1000, TimeUnit.MILLISECONDS).size());
    //verify that client was given resultsby request sink, and that context was closed
    verify(requestContext, times(3)).addResponse(isA(Message.class));
  }

  @Test
  public void testFragmentedResponse() throws InterruptedException, TimeoutException, ExecutionException {
    serverContainer.initialize();
    clientContainer.initialize();
    //when endpoint receives signal, it replies with a huge reply
    mockEndpointSignal(createBigResponse());
    //mock client handling of response
    Future<List<TestMessage>> response = mockReceiveResponse();

    //do request
    requestSink.signal(new TestMessage("request"), requestContext, 10000);
    //wait for replies
    assertEquals(1, response.get(1000, TimeUnit.MILLISECONDS).size());
  }

  @Test
  public void testSignalSequence() throws InterruptedException, TimeoutException, ExecutionException {
    serverContainer.initialize();
    clientContainer.initialize();
    Semaphore sem = new Semaphore(0);
    doAnswer(i -> {
      sem.release();
      return null;
    }).when(requestContext).endOfStream();
    when(endpoint.signal(isA(TestMessage.class), isA(RequestContext.class), anyLong())).thenAnswer(i -> {
      RequestContext ctx = (RequestContext) i.getArguments()[1];
      ctx.addResponse(new TestMessage("reploy"));
      ctx.endOfStream();
      return ctx;
    });

    //do first request
    requestSink.signal(new TestMessage("request"), requestContext, 10000);
    //verify that reply was received and context closed
    assertTrue(sem.tryAcquire(1000, TimeUnit.MILLISECONDS));
    verify(requestContext, times(1)).addResponse(isA(Message.class));
    verify(endpoint, times(1)).signal(any(), any(), anyLong());

    //do second request
    requestSink.signal(new TestMessage("request"), requestContext, 10000);
    //verify that reply was received and context closed
    assertTrue(sem.tryAcquire(1000, TimeUnit.MILLISECONDS));
    verify(requestContext, times(2)).addResponse(isA(Message.class));
    verify(endpoint, times(2)).signal(any(), any(), anyLong());
  }

  //helpers

  private Future<TestMessage> mockEndpointSignal(TestMessage... replies) {
    CompletableFuture<TestMessage> signal = new CompletableFuture<>();
    when(endpoint.signal(isA(TestMessage.class), isA(RequestContext.class), anyLong())).thenAnswer(i -> {
      TestMessage msg = i.getArgument(0);
      RequestContext ctx = (RequestContext) i.getArguments()[1];
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
    when(requestContext.addResponse(isA(TestMessage.class))).thenAnswer(i -> {
      if (endOfStream.isDone()) throw new IllegalStateException("Received response to closed client");
      responses.add(i.getArgument(0));
      return true;
    });
    doAnswer(i -> endOfStream.complete(responses)).when(requestContext).endOfStream();
    return endOfStream;
  }

}
