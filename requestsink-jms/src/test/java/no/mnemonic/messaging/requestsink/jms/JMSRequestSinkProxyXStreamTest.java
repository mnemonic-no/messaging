package no.mnemonic.messaging.requestsink.jms;

import com.thoughtworks.xstream.io.binary.BinaryStreamDriver;
import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.ResponseListener;
import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import no.mnemonic.messaging.requestsink.jms.serializer.XStreamMessageSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class JMSRequestSinkProxyXStreamTest extends AbstractJMSRequestSinkProxyTest {


  @Before
  public void setUp() {

    //create mock client (requestor to requestSink) and endpoint (target for requestProxy)
    endpoint = mock(RequestSink.class);
    requestContext = mock(RequestContext.class);
    clientResponseListener = mock(ResponseListener.class);
    serverResponseListener = mock(ResponseListener.class);

    //set up a real JMS connection to a vm-local activemq
    queueName = "dynamicQueues/" + generateCookie(10);
    topicName = "dynamicTopics/" + generateCookie(10);

    //set up request sink pointing at a vm-local topic
    requestSink = addConnection(JMSRequestSink.builder())
            .setProtocolVersion(ProtocolVersion.V4)
            .setSerializer(createXstream())
            .setQueueName(queueName)
            .setTopicName(topicName)
            .build();

    //set up request proxy listening to the vm-local topic, and pointing to mock endpoint
    requestProxy = addConnection(JMSRequestProxy.builder())
            .addSerializer(new DefaultJavaMessageSerializer())
            .addSerializer(createXstream())
            .setMaxMessageSize(1000)
            .setQueueName(queueName)
            .setTopicName(topicName)
            .setRequestSink(endpoint)
            .build();

    clientContainer = ComponentContainer.create(requestSink);
    serverContainer = ComponentContainer.create(requestProxy);
  }

  @After
  public void tearDown() {
    clientContainer.destroy();
    serverContainer.destroy();
    executor.shutdown();
  }

  @Test
  public void testSignalUsingIllegalClass() throws Exception {
    serverContainer.initialize();
    clientContainer.initialize();
    //mock client handling of response
    Future<IllegalDeserializationException> future = mockNotifyError();
    requestSink.signal(new IllegalMessage(), requestContext, 10000);
    IllegalDeserializationException e = future.get(2, TimeUnit.SECONDS);
    assertNotNull(e);
  }

  @Test
  public void testResponseUsingIllegalClass() throws Exception {
    serverContainer.initialize();
    clientContainer.initialize();
    //when endpoint receives signal, it replies with a single illegal reply
    mockEndpointSignal(new IllegalMessage());
    //mock client handling of response
    Future<IllegalDeserializationException> future = mockNotifyError();
    requestSink.signal(new TestMessage("message"), requestContext, 10000);
    IllegalDeserializationException e = future.get(1, TimeUnit.SECONDS);
    assertNotNull(e);
  }

  @Test
  public void testAcknowledgeResponse() throws Exception {
    serverContainer.initialize();
    clientContainer.initialize();
    Semaphore acksem = new Semaphore(0);

    //mock receiving ack back to server
    doAnswer(i->{
      acksem.release();
      return null;
    }).when(serverResponseListener).responseAccepted();

    //mock client handling of response
    when(requestContext.addResponse(any(), any())).thenAnswer(i -> {
      ResponseListener l = i.getArgument(1);
      l.responseAccepted();
      return true;
    });

    //when endpoint receives signal, it replies with two replies
    mockEndpointSignal(new TestMessage("reply1"), new TestMessage("reply2"));
    requestSink.signal(new TestMessage("request"), requestContext, 10000);

    //wait for two acknowledgements to be received serverside
    assertTrue(acksem.tryAcquire(2, 2, TimeUnit.SECONDS));
    verify(requestContext, times(2)).addResponse(any(), any());
    verify(serverResponseListener, times(2)).responseAccepted();

  }

  private XStreamMessageSerializer createXstream() {
    return XStreamMessageSerializer.builder()
            .addAllowedClass(TestMessage.class)
            .setDriver(new BinaryStreamDriver())
            .build();
  }

  public static class IllegalMessage implements Message {

    private static final long serialVersionUID = -8911834432780676086L;

    @Override
    public long getMessageTimestamp() {
      return 0;
    }

    @Override
    public String getCallID() {
      return "callID";
    }

  }

}
