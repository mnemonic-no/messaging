package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestListener;
import no.mnemonic.messaging.requestsink.RequestSink;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static no.mnemonic.messaging.requestsink.jms.JMSRequestProxy.*;
import static no.mnemonic.messaging.requestsink.jms.JMSUtils.assertNotNull;

/**
 * The server response context is the context object sent to the server side RequestSink along with the signal received from the client.
 * It will encode all responses into JMS messages and submit back to client.
 * When RequestSink closes stream or notifies an exception, this will also be notified to the client.
 * <p>
 * Multiple responses will be encoded as multiple messages, creating a response stream back to the client.
 * When channel is closed, responses will be ignored.
 */
class ServerResponseContext implements RequestContext, ServerContext {

  private static final Logger LOGGER = Logging.getLogger(ServerResponseContext.class);
  private static Clock clock = Clock.systemUTC();

  private final Session session;
  private final MessageProducer replyProducer;
  private final Destination replyTo;
  private final String callID;
  private final AtomicLong timeout = new AtomicLong();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final ProtocolVersion protocolVersion;
  private final int maxMessageSize;
  private final ServerMetrics metrics;

  ServerResponseContext(String callID, Session session, MessageProducer replyProducer, Destination replyTo, long timeout, ProtocolVersion protocolVersion, int maxMessageSize, ServerMetrics metrics) throws NamingException, JMSException {
    this.callID = assertNotNull(callID, "CallID not set");
    this.session = assertNotNull(session, "session not set");
    this.replyProducer = assertNotNull(replyProducer, "replyProducer not set");
    this.replyTo = assertNotNull(replyTo, "replyTo not set");
    this.protocolVersion = assertNotNull(protocolVersion, "ProtocolVersion not set");
    this.metrics = assertNotNull(metrics, "metrics not set");
    if (maxMessageSize <= 1) throw new IllegalArgumentException("MaxMessageSize must be a positive integer");
    this.maxMessageSize = maxMessageSize;
    if (timeout <= 0) throw new IllegalArgumentException("Timeout must be a positive integer");
    this.timeout.set(timeout);
  }

  /**
   * Method to implement {@link ServerChannelUploadContext.UploadHandler}
   */
  void handle(RequestSink requestSink, Message request) throws JMSException {
    assertNotNull(requestSink, "RequestSink not set");
    assertNotNull(request, "Message not set");
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< signal [callID=%s replyTo=%s]", callID, replyTo);
    }
    requestSink.signal(request, this, clock.millis() - timeout.get());
  }

  public boolean keepAlive(long until) {
    //if channel is closed, do not accept keepalive request
    if (isClosed()) {
      return false;
    }
    //if keepalive requests to extend timeout, relay that request back to client
    try {
      //create a extend-wait message to client (message content has no meaning)
      javax.jms.Message closeMessage = JMSUtils.createTextMessage(session, "please wait", protocolVersion);
      closeMessage.setJMSCorrelationID(callID);
      closeMessage.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_EXTEND_WAIT);
      closeMessage.setLongProperty(PROPERTY_REQ_TIMEOUT, until);
      metrics.extendWait();
      replyProducer.send(replyTo, closeMessage);
      if (LOGGER.isDebug()) {
        LOGGER.debug(">> keepalive [callID=%s until=%s replyTo=%s]", callID, new Date(until), replyTo);
      }
    } catch (Exception e) {
      LOGGER.warning("Could not send Extend-Wait for " + callID);
    }
    timeout.set(until);
    return true;
  }

  public boolean addResponse(Message msg) {
    // drop message if we're closed
    if (isClosed()) {
      return false;
    }

    try {
      byte[] messageBytes = JMSUtils.serialize(msg);
      //if request origin is sending using protocol V2 or higher, fragmented responses are supported, so fragment big responses
      if (protocolVersion.atLeast(ProtocolVersion.V2) && messageBytes.length > maxMessageSize) {
        sendResponseFragments(messageBytes);
      } else {
        sendSingleResponse(messageBytes);
      }
      metrics.reply();
      return true;
    } catch (Exception e) {
      LOGGER.error(e, "Error adding response for " + callID);
      close();
      return false;
    }
  }

  private void sendSingleResponse(byte[] messageBytes) throws JMSException, IOException {
    // construct single response message
    javax.jms.Message returnMessage = JMSUtils.createByteMessage(session, messageBytes, protocolVersion);
    returnMessage.setJMSCorrelationID(callID);
    returnMessage.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_SIGNAL_RESPONSE);
    // send return message
    replyProducer.send(replyTo, returnMessage);
    if (LOGGER.isDebug()) {
      LOGGER.debug(">> addResponse [callID=%s size=%d replyTo=%s]", callID, messageBytes.length, replyTo);
    }
  }

  private void sendResponseFragments(byte[] messageBytes) throws JMSException, IOException {
    UUID responseID = UUID.randomUUID();
    try (InputStream messageDataStream = new ByteArrayInputStream(messageBytes)) {

      JMSUtils.fragment(messageDataStream, maxMessageSize, new JMSUtils.FragmentConsumer() {
        @Override
        public void fragment(byte[] data, int idx) throws JMSException, IOException {
          BytesMessage fragment = JMSUtils.createByteMessage(session, data, protocolVersion);
          fragment.setJMSCorrelationID(callID);
          fragment.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_SIGNAL_FRAGMENT);
          fragment.setStringProperty(PROPERTY_RESPONSE_ID, responseID.toString());
          fragment.setIntProperty(PROPERTY_FRAGMENTS_IDX, idx);
          //send fragment to upload channel
          replyProducer.send(replyTo, fragment);
          metrics.fragmentReplyFragment();
          if (LOGGER.isDebug()) {
            LOGGER.debug(">> addFragmentedResponse [callID=%s responseID=%s idx=%d size=%d replyTo=%s]", callID, responseID, idx, data.length, replyTo);
          }
        }

        @Override
        public void end(int fragments, byte[] digest) throws JMSException {
          //prepare EOS message (message text has no meaning)
          javax.jms.Message eof = JMSUtils.createTextMessage(session, "End-Of-Stream", protocolVersion);
          eof.setJMSCorrelationID(callID);
          eof.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_END_OF_FRAGMENTED_MESSAGE);
          eof.setStringProperty(PROPERTY_RESPONSE_ID, responseID.toString());
          //send total number of fragments and message digest with EOS message, to allow receiver to verify
          eof.setIntProperty(PROPERTY_FRAGMENTS_TOTAL, fragments);
          eof.setStringProperty(PROPERTY_DATA_CHECKSUM_MD5, JMSUtils.hex(digest));
          //send EOS
          replyProducer.send(replyTo, eof);
          metrics.fragmentedReplyCompleted();
          if (LOGGER.isDebug()) {
            LOGGER.debug(">> fragmentedResponse EOF [callID=%s responseID=%s fragments=%d replyTo=%s]", callID, responseID, fragments, replyTo);
          }
        }
      });
    }
  }

  private void close() {
    closed.set(true);
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
        exMessage.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_EXCEPTION);
        replyProducer.send(replyTo, exMessage);
        metrics.exceptionSignal();
        if (LOGGER.isDebug()) {
          LOGGER.debug(">> notifyErrorToClient [callID=%s exception=%s replyTo=%s]", callID, e.getClass(), replyTo);
        }
      } catch (Exception e1) {
        LOGGER.warning("Could not send error notification for " + callID);
        close();
      }
    }
  }

  public void endOfStream() {
    if (!isClosed()) {
      try {
        //send EndOfStream message (message text has no meaning)
        javax.jms.Message closeMessage = JMSUtils.createTextMessage(session, "stream closed", protocolVersion);
        closeMessage.setJMSCorrelationID(callID);
        closeMessage.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_STREAM_CLOSED);
        replyProducer.send(replyTo, closeMessage);
        metrics.endOfStream();
        if (LOGGER.isDebug()) {
          LOGGER.debug(">> endOfStream [callID=%s replyTo=%s]", callID, replyTo);
        }
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
