package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestListener;
import no.mnemonic.messaging.requestsink.RequestSink;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.NamingException;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static no.mnemonic.messaging.requestsink.jms.JMSUtils.assertNotNull;

class ServerResponseContext implements RequestContext, JMSRequestProxy.ServerContext {

  private static final Logger LOGGER = Logging.getLogger(ServerResponseContext.class);
  private static Clock clock = Clock.systemUTC();

  private final Session session;
  private final MessageProducer replyTo;
  private final String callID;
  private final AtomicLong timeout = new AtomicLong();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final ProtocolVersion protocolVersion;

  ServerResponseContext(String callID, Session session, Destination replyTo, long timeout, ProtocolVersion protocolVersion) throws NamingException, JMSException {
    this.callID = assertNotNull(callID, "CallID not set");
    this.session = assertNotNull(session, "Session not set");
    this.protocolVersion = assertNotNull(protocolVersion, "ProtocolVersion not set");
    if (timeout <= 0) throw new IllegalArgumentException("Timeout must be a positive integer");
    this.timeout.set(timeout);
    //create a producer to send responses back to client
    this.replyTo = session.createProducer(replyTo);
  }

  /**
   * Method to implement {@link ServerChannelUploadContext.UploadHandler}
   */
  void handle(RequestSink requestSink, Message request) throws JMSException {
    assertNotNull(requestSink, "RequestSink not set");
    assertNotNull(request, "Message not set");
    requestSink.signal(request, this, clock.millis() - timeout.get());
  }

  public boolean keepAlive(long until) {
    //if channel is closed, do not accept keepalive request
    if (isClosed()) {
      return false;
    }
      //if keepalive requests to extend timeout, relay that request back to client
    else if (until > timeout.get()) {
      try {
        //create a extend-wait message to client
        javax.jms.Message closeMessage = JMSUtils.createTextMessage(session, "please wait", protocolVersion);
        closeMessage.setJMSCorrelationID(callID);
        closeMessage.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_EXTEND_WAIT);
        closeMessage.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, until);
        replyTo.send(closeMessage);
      } catch (Exception e) {
        LOGGER.warning("Could not send Extend-Wait for " + callID);
      }
      timeout.set(until);
    }
    return true;
  }

  public boolean addResponse(Message msg) {
    // drop message if we're closed
    if (isClosed()) {
      return false;
    }

    try {
      // construct return message
      javax.jms.Message returnMessage = JMSUtils.createByteMessage(session, JMSUtils.serialize(msg), protocolVersion);
      returnMessage.setJMSCorrelationID(callID);
      returnMessage.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_SIGNAL_RESPONSE);
      // send return message
      replyTo.send(returnMessage);
      return true;
    } catch (Exception e) {
      LOGGER.error(e, "Error adding response for " + callID);
      close();
      return false;
    }
  }

  private void close() {
    closed.set(true);
    JMSUtils.closeProducer(replyTo);
  }

  public boolean isClosed() {
    if (closed.get()) {
      return true;
    } else if (System.currentTimeMillis() > timeout.get()) {
      // we claim to be closed if this sink has timed out
      //but make sure it actually is closed
      try {
        close();
      } catch (Exception e) {
        LOGGER.warning(e, "Error closing response sink");
      }
      return true;
    } else {
      return false;
    }
  }

  public void notifyError(Throwable e) {
    if (!isClosed()) {
      try {
        ExceptionMessage ex = new ExceptionMessage(callID, e);
        javax.jms.Message exMessage = JMSUtils.createByteMessage(session, JMSUtils.serialize(ex), protocolVersion);
        exMessage.setJMSCorrelationID(callID);
        exMessage.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_EXCEPTION);
        replyTo.send(exMessage);
      } catch (Exception e1) {
        LOGGER.warning("Could not send error notification for " + callID);
        close();
      }
    }
  }

  public void endOfStream() {
    if (!isClosed()) {
      try {
        javax.jms.Message closeMessage = JMSUtils.createTextMessage(session, "stream closed", protocolVersion);
        closeMessage.setJMSCorrelationID(callID);
        closeMessage.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED);
        replyTo.send(closeMessage);
      } catch (Exception e) {
        LOGGER.warning("Could not send End-Of-Stream for " + callID);
      } finally {
        close();
      }
    }
  }

  public void addListener(RequestListener listener) {
    //do nothing
  }

  public void removeListener(RequestListener listener) {
    //do nothing
  }
}
