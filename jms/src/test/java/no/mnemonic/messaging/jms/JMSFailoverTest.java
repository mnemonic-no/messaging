package no.mnemonic.messaging.jms;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.commons.testtools.AvailablePortFinder;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.api.RequestSink;
import no.mnemonic.messaging.api.SignalContext;
import no.mnemonic.messaging.api.SignalHandler;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

public class JMSFailoverTest {

  private BrokerService broker1;
  private BrokerService broker2;
  private ComponentContainer container;
  private int port1, port2;

  @Mock
  private RequestSink mockedSink;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);

    port1 = AvailablePortFinder.getAvailablePort(10000);
    port2 = AvailablePortFinder.getAvailablePort(11000);

    broker1 = setupBroker(port1, port2);
    broker2 = setupBroker(port2, port1);

    when(mockedSink.signal(any(), any(), anyLong())).thenAnswer(i -> {
      ((SignalContext) i.getArgument(1)).endOfStream();
      return null;
    });
  }

  @After
  public void cleanup() {
    container.destroy();
    LambdaUtils.tryTo(() -> broker1.stop());
    LambdaUtils.tryTo(() -> broker2.stop());
  }

  @Test
  public void testConnect() {
    JMSConnection conn = createFailoverConnection();
    container = ComponentContainer.create(conn);
    container.initialize();
  }

  @Test
  public void testFailoverBetweenServers() throws Exception {
    JMSRequestSink sink = setupFailoverServerAndClient();
    System.out.println("Starting");

    assertTrue(new SignalHandler(sink, new TestMessage("msg"), true, 10000).waitForEndOfStream(10000));

    broker1.stop();

    assertTrue(new SignalHandler(sink, new TestMessage("msg"), true, 10000).waitForEndOfStream(10000));

    broker1 = setupBroker(port1, port2);
    broker2.stop();

    assertTrue(new SignalHandler(sink, new TestMessage("msg"), true, 10000).waitForEndOfStream(10000));

    broker1.stop();

  }

  private JMSRequestSink setupFailoverServerAndClient() throws InterruptedException, ExecutionException, TimeoutException {
    JMSConnection serverconn = createFailoverConnection();
    JMSConnection clientconn = createFailoverConnection();

    JMSRequestProxy proxy = createProxy(serverconn, mockedSink);
    JMSRequestSink sink = createSink(clientconn);

    CompletableFuture<Void> proxyconnected = new CompletableFuture<>();
    proxy.addJMSRequestProxyConnectionListener(l->{
      proxyconnected.complete(null);
      System.out.println("***** Proxy reconnected *****");
    });

    container = ComponentContainer.create(serverconn, clientconn, proxy, sink);
    container.initialize();
    proxyconnected.get(1000, TimeUnit.MILLISECONDS);
    return sink;
  }

  //helpers


  private BrokerService setupBroker(int thisPort, int otherPort) throws Exception {
    BrokerService broker = new BrokerService();
    broker.setPersistent(false);
    broker.setBrokerName("broker" + thisPort);
    broker.addConnector("tcp://0.0.0.0:" + thisPort);
    broker.addNetworkConnector(String.format("static:(tcp://localhost:%d)", otherPort));
    broker.start();
    broker.waitUntilStarted();
    return broker;
  }

  private JMSRequestSink createSink(JMSConnection connection) {
    return JMSRequestSink.builder()
            .addConnection(connection)
            .setProtocolVersion(ProtocolVersion.V16)
            .setDestinationName("dynamicQueues/testqueue")
            .build();
  }


  private JMSRequestProxy createProxy(JMSConnection connection, RequestSink endpoint) {
    //set up request sink pointing at a vm-local topic
    return JMSRequestProxy.builder()
            .addConnection(connection)
            .setDestinationName("dynamicQueues/testqueue")
            .setRequestSink(endpoint)
            .build();
  }

  private JMSConnection createFailoverConnection() {
    //set up a real JMS connection to a vm-local activemq
    return JMSConnectionImpl.builder()
            .setContextFactoryName("org.apache.activemq.jndi.ActiveMQInitialContextFactory")
            .setContextURL(String.format("failover:(tcp://localhost:%d,tcp://localhost:%d)?initialReconnectDelay=100", port1, port2))
            .setConnectionFactoryName("ConnectionFactory")
            .build();
  }
  private JMSConnection createSingleConnection(int port) {
    //set up a real JMS connection to a vm-local activemq
    return JMSConnectionImpl.builder()
            .setContextFactoryName("org.apache.activemq.jndi.ActiveMQInitialContextFactory")
            .setContextURL(String.format("tcp://localhost:%d", port))
            .setConnectionFactoryName("ConnectionFactory")
            .build();
  }
}
