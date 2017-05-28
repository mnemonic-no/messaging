package no.mnemonic.messaging.jms;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.messaging.api.RequestSink;
import no.mnemonic.messaging.api.SignalContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;


/**
 * @author joakim
 */
public class JMSMultiResponseTest extends AbstractJMSRequestTest {

  private JMSRequestSink requestSink;
  private JMSRequestProxy requestProxy;

  @Mock
  private RequestSink endpoint;
  @Mock
  private SignalContext signalContext;

  private ComponentContainer container;
  private AtomicBoolean finished = new AtomicBoolean();
  private AtomicReference<Throwable> error = new AtomicReference<>();
  private CompletableFuture<List<TestMessage>> resolved = new CompletableFuture<>();


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    JMSConnection connection = createConnection();
    String queueName = "dynamicQueues/" + generateCookie(10);

    requestSink = JMSRequestSink.builder()
            .addConnection(connection)
            .setDestinationName(queueName)
            .setExecutor(r -> executor.submit(r))
            .build();

    //set up request sink pointing at a vm-local topic
    requestProxy = JMSRequestProxy.builder()
            .addConnection(connection)
            .setDestinationName(queueName)
            .setExecutor(r -> executor.submit(r))
            .setRequestSink(endpoint)
            .build();

    container = ComponentContainer.create(connection, requestProxy, requestSink);
    container.initialize();

    //mock common
    mockEndpoint();
  }

  @After
  public void tearDown() throws Exception {
    container.destroy();
  }

  @Test
  public void testMassiveResponseSignalWithException() throws Exception {

    TestMessage request = new TestMessage("req");
    TestMessage reply = new TestMessage("resp");

    when(endpoint.signal(any(), any(), anyLong())).thenAnswer(i -> {
      SignalContext ctx = i.getArgument(1);
      executor.submit(() -> {
        for (int i1 = 0; i1 < 100; i1++) {
          ctx.addResponse(reply);
        }
        ctx.notifyError(new RuntimeException("test"));
        ctx.endOfStream();
      });
      return ctx;
    });

    requestSink.signal(request, signalContext, 30000);
    List<TestMessage> result = resolved.get(1000, TimeUnit.MILLISECONDS);
    assertEquals(100, result.size());
    assertTrue(finished.get());
    assertEquals("test", error.get().getMessage());
  }

  @Test
  public void testMassiveResponseSignal() throws Exception {

    TestMessage request = new TestMessage("req");
    TestMessage reply = new TestMessage("resp");

    when(endpoint.signal(any(), any(), anyLong())).thenAnswer(i -> {
      SignalContext endpointSignalContext = i.getArgument(1);
      executor.submit(() -> {
        System.out.println("Submitting responses");
        for (int i1 = 0; i1 < 100; i1++) {
          endpointSignalContext.addResponse(reply);
        }
        endpointSignalContext.endOfStream();
        System.out.println("Sent EOS");
      });
      return endpointSignalContext;
    });

    requestSink.signal(request, signalContext, 30000);
    List<TestMessage> result = resolved.get(1000, TimeUnit.MILLISECONDS);
    assertEquals(100, result.size());
    assertTrue(finished.get());
    assertNull(error.get());
  }

  private void mockEndpoint() {
    List<TestMessage> responses = new ArrayList<>();
    when(signalContext.addResponse(any())).thenAnswer(i -> {
      responses.add(i.getArgument(0));
      return true;
    });
    when(signalContext.isClosed()).thenAnswer(i -> finished.get());
    doAnswer(i -> {
      error.set(i.getArgument(0));
      return null;
    }).when(signalContext).notifyError(any());
    doAnswer(i -> {
      finished.set(true);
      resolved.complete(responses);
      return null;
    }).when(signalContext).endOfStream();
  }

}
