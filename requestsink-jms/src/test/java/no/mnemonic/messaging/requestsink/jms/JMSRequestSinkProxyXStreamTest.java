package no.mnemonic.messaging.requestsink.jms;

import com.thoughtworks.xstream.io.binary.BinaryStreamDriver;
import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import no.mnemonic.messaging.requestsink.jms.serializer.XStreamMessageSerializer;
import org.junit.After;
import org.junit.Before;

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

  private XStreamMessageSerializer createXstream() {
    return XStreamMessageSerializer.builder()
            .addAllowedClass("no.mnemonic.*")
            .setDriver(new BinaryStreamDriver())
            .build();
  }


}
