package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import org.junit.After;
import org.junit.Before;

import static org.mockito.Mockito.mock;

public class JMSRequestSinkJavaSerializationProxyTest extends AbstractJMSRequestSinkProxyTest {


  @Before
  public void setUp() throws Exception {

    //create mock client (requestor to requestSink) and endpoint (target for requestProxy)
    endpoint = mock(RequestSink.class);
    requestContext = mock(RequestContext.class);

    //set up a real JMS connection to a vm-local activemq
    queueName = "dynamicQueues/" + generateCookie(10);
    topicName = "dynamicTopics/" + generateCookie(10);

    //set up request sink pointing at a vm-local topic
    requestSink = addConnection(JMSRequestSink.builder())
            .setProtocolVersion(ProtocolVersion.V3)
            .setSerializer(new DefaultJavaMessageSerializer())
            .setQueueName(queueName)
            .setTopicName(topicName)
            .build();

    //set up request proxy listening to the vm-local topic, and pointing to mock endpoint
    requestProxy = addConnection(JMSRequestProxy.builder())
            .addSerializer(new DefaultJavaMessageSerializer())
            .setMaxMessageSize(1000)
            .setQueueName(queueName)
            .setTopicName(topicName)
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

}
