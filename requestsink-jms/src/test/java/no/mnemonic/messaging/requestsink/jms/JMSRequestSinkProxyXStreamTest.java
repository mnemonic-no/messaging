package no.mnemonic.messaging.requestsink.jms;

import com.thoughtworks.xstream.io.binary.BinaryStreamDriver;
import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import no.mnemonic.messaging.requestsink.jms.serializer.XStreamMessageSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class JMSRequestSinkProxyXStreamTest extends AbstractJMSRequestSinkProxyTest {


  @Before
  public void setUp() throws Exception {

    //create mock client (requestor to requestSink) and endpoint (target for requestProxy)
    endpoint = mock(RequestSink.class);
    requestContext = mock(RequestContext.class);

    //set up a real JMS connection to a vm-local activemq
    queueName = "dynamicQueues/" + generateCookie(10);

    //set up request sink pointing at a vm-local topic
    requestSink = addConnection(JMSRequestSink.builder())
            .setProtocolVersion(ProtocolVersion.V3)
            .setSerializer(createXstream())
            .setDestinationName(queueName)
            .build();

    //set up request proxy listening to the vm-local topic, and pointing to mock endpoint
    requestProxy = addConnection(JMSRequestProxy.builder())
            .addSerializer(new DefaultJavaMessageSerializer())
            .addSerializer(createXstream())
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
