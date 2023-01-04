package no.mnemonic.messaging.requestsink.jms.context;

import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.jms.ProtocolVersion;
import no.mnemonic.messaging.requestsink.jms.serializer.MessageSerializer;
import no.mnemonic.messaging.requestsink.jms.util.ServerMetrics;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import java.time.Clock;

import static no.mnemonic.messaging.requestsink.jms.AbstractJMSRequestBase.MESSAGE_TYPE_EXTEND_WAIT;
import static no.mnemonic.messaging.requestsink.jms.AbstractJMSRequestBase.PROPERTY_MESSAGE_TYPE;
import static no.mnemonic.messaging.requestsink.jms.AbstractJMSRequestBase.PROPERTY_REQ_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ServerResponseContextTest {

  private static final String CALL_ID = "callID";
  private static final long TIMEOUT = 20000;
  private static final long NOW = 10000;
  private static final int MAX_MESSAGE_SIZE = 10000;

  @Mock
  private Session session;
  @Mock
  private MessageProducer messageProducer;
  @Mock
  private Destination replyTo;
  @Mock
  private MessageSerializer serializer;
  @Mock
  private ServerMetrics metrics;
  @Mock
  private RequestSink requestSink;
  @Mock
  private Clock clock;

  private ServerResponseContext context;


  @BeforeEach
  public void prepare() throws JMSException {
    context = new ServerResponseContext(
            CALL_ID, session, messageProducer, replyTo, TIMEOUT, ProtocolVersion.V3,
            MAX_MESSAGE_SIZE, metrics, serializer, requestSink);
    when(session.createTextMessage(any())).thenReturn(new ActiveMQTextMessage());
    when(clock.millis()).thenReturn(NOW);
    ServerResponseContext.setClock(clock);
  }

  @Test
  void testKeepAlive() throws JMSException {
    assertTrue(context.keepAlive(NOW+1000));
    verify(metrics).extendWait();
    verify(messageProducer).send(same(replyTo), argThat(m -> LambdaUtils.tryResult(()->
            m.getJMSCorrelationID().equals(CALL_ID)
            && m.getStringProperty(PROPERTY_MESSAGE_TYPE).equals(MESSAGE_TYPE_EXTEND_WAIT)
            && m.getLongProperty(PROPERTY_REQ_TIMEOUT) == NOW+1000,
            false)));
  }

  @Test
  void testKeepAliveContinuesOnRandomException() throws JMSException {
    doThrow(RuntimeException.class).when(messageProducer).send(same(replyTo), any());
    assertTrue(context.keepAlive(NOW+1000));
  }

  @Test
  void testKeepAliveFailsOnDestinationError() throws JMSException {
    doThrow(InvalidDestinationException.class).when(messageProducer).send(same(replyTo), any());
    assertFalse(context.keepAlive(NOW+1000));
  }

  @Test
  void testClose() {
    context.close();
    assertTrue(context.isClosed());
    verify(requestSink).abort(CALL_ID);
  }
}