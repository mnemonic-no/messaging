package no.mnemonic.messaging.requestsink.jms.context;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.MessagingInterruptedException;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestListener;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.ResponseListener;
import no.mnemonic.messaging.requestsink.jms.ExceptionMessage;
import no.mnemonic.messaging.requestsink.jms.ProtocolVersion;
import no.mnemonic.messaging.requestsink.jms.serializer.MessageSerializer;
import no.mnemonic.messaging.requestsink.jms.util.FragmentConsumer;
import no.mnemonic.messaging.requestsink.jms.util.ServerMetrics;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.messaging.requestsink.jms.JMSRequestProxy.*;
import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.*;

/**
 * The server response context is the context object sent to the server side RequestSink along with the signal received from the client.
 * It will encode all responses into JMS messages and submit back to client.
 * When RequestSink closes stream or notifies an exception, this will also be notified to the client.
 * <p>
 * Multiple responses will be encoded as multiple messages, creating a response stream back to the client.
 * When channel is closed, responses will be ignored.
 */
public class ServerResponseContext implements RequestContext, ServerContext {

  private static final int DEFAULT_MAX_WINDOW_ATTEMPTS = 10;
  private static final int DEFAULT_WINDOW_WAIT_SECONDS = 10;

  private static final Logger LOGGER = Logging.getLogger(ServerResponseContext.class);
  private static Clock clock = Clock.systemUTC();

  private final Session session;
  private final MessageProducer replyProducer;
  private final Destination replyTo;
  private final Destination acknowledgementTo;
  private final String callID;
  private final UUID serverNodeID;
  private final AtomicLong timeout = new AtomicLong();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final ProtocolVersion protocolVersion;
  private final int maxMessageSize;
  private final int maxWindowAttempts;
  private final int maxWindowWaitSeconds;
  private final ServerMetrics metrics;
  private final MessageSerializer serializer;
  private final RequestSink requestSink;
  private final AtomicReference<ResponseListener> lastResponseListener = new AtomicReference<>();

  private final Semaphore segmentWindow;

  private ServerResponseContext(
      String callID,
      Session session,
      MessageProducer replyProducer,
      Destination replyTo,
      Destination acknowledgementTo,
      long timeout,
      UUID serverNodeID,
      ProtocolVersion protocolVersion,
      int maxMessageSize,
      int maxWindowAttempts,
      int maxWindowWaitSeconds,
      int segmentWindowSize,
      ServerMetrics metrics,
      MessageSerializer serializer,
      RequestSink requestSink) {
    if (segmentWindowSize < 1) throw new IllegalArgumentException("segmentWindowSize must be at least 1");
    if (maxWindowAttempts < 1) throw new IllegalArgumentException("maxWindowAttempts must be at least 1");
    if (maxWindowWaitSeconds < 1) throw new IllegalArgumentException("maxWindowWaitSeconds must be at least 1");
    this.serverNodeID = serverNodeID;
    this.maxWindowAttempts = maxWindowAttempts;
    this.maxWindowWaitSeconds = maxWindowWaitSeconds;
    this.callID = assertNotNull(callID, "CallID not set");
    this.session = assertNotNull(session, "session not set");
    this.replyProducer = assertNotNull(replyProducer, "replyProducer not set");
    this.replyTo = assertNotNull(replyTo, "replyTo not set");
    this.acknowledgementTo = assertNotNull(acknowledgementTo, "acknowledgementTo not set");
    this.protocolVersion = assertNotNull(protocolVersion, "ProtocolVersion not set");
    this.metrics = assertNotNull(metrics, "metrics not set");
    this.serializer = assertNotNull(serializer, "serializer not set");
    this.requestSink = assertNotNull(requestSink, "requestSink not set");
    if (maxMessageSize <= 1) throw new IllegalArgumentException("MaxMessageSize must be a positive integer");
    if (timeout <= 0) throw new IllegalArgumentException("Timeout must be a positive integer");
    this.maxMessageSize = maxMessageSize;
    this.timeout.set(timeout);
    this.segmentWindow = new Semaphore(segmentWindowSize);
  }

  /**
   * Method to implement {@link ServerChannelUploadContext.UploadHandler}
   */
  public void handle(Message request) {
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
      javax.jms.Message closeMessage = createTextMessage(session, "please wait", protocolVersion);
      closeMessage.setJMSCorrelationID(callID);
      closeMessage.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_EXTEND_WAIT);
      closeMessage.setLongProperty(PROPERTY_REQ_TIMEOUT, until);
      replyProducer.send(replyTo, closeMessage);
      metrics.extendWait();
      if (LOGGER.isDebug()) {
        LOGGER.debug(">> keepalive [callID=%s until=%s replyTo=%s]", callID, new Date(until), replyTo);
      }
    } catch (InvalidDestinationException e) {
      LOGGER.warning(e, "Cannot keep connection alive for %s, response channel invalid.", callID);
      return false;
    } catch (Exception e) {
      LOGGER.warning(e, "Could not send Extend-Wait for " + callID);
    }
    timeout.set(until);
    return true;
  }

  @Deprecated
  public boolean addResponse(Message msg) {
    return this.addResponse(msg, () -> {
    });
  }

  @Override
  public boolean addResponse(Message msg, ResponseListener responseListener) {

    //for V4 protocol, require free segment window to add another response
    int windowAttempts = 0;
    while (protocolVersion.atLeast(ProtocolVersion.V4) && !isClosed()) {
      try {
        if (segmentWindow.tryAcquire(maxWindowWaitSeconds, TimeUnit.SECONDS)) {
          break;
        } else {
          windowAttempts++;
          if (windowAttempts > maxWindowAttempts) {
            throw new IllegalStateException(String.format("Could not acquire from segment window after %d attempts", maxWindowAttempts));
          }
          LOGGER.warning("Throttling sending segments for callID %s after %d seconds", callID, maxWindowWaitSeconds);
        }
      } catch (InterruptedException e) {
        LOGGER.error(e, "Interrupted waiting for segmentWindow " + callID);
        close();
        throw new MessagingInterruptedException(e);
      }
    }
    // drop message if we're closed
    if (isClosed()) {
      return false;
    }

    if (responseListener != null) lastResponseListener.set(responseListener);

    try {
      byte[] messageBytes = serializer.serialize(msg);
      //if request origin is sending using protocol V2 or higher, fragmented responses are supported, so fragment big responses
      if (messageBytes.length > maxMessageSize) {
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

  @Override
  public void abort() {
    requestSink.abort(callID);
    close();
  }

  //private methods
  private void close() {
    closed.set(true);
  }

  private void sendSingleResponse(byte[] messageBytes) throws JMSException {
    // construct single response message
    javax.jms.Message returnMessage = createByteMessage(session, messageBytes, protocolVersion, serializer.serializerID());
    returnMessage.setJMSCorrelationID(callID);
    returnMessage.setJMSReplyTo(acknowledgementTo);
    returnMessage.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_SIGNAL_RESPONSE);
    returnMessage.setStringProperty(PROPERTY_SERVER_NODE_ID, serverNodeID.toString());
    // send return message
    replyProducer.send(replyTo, returnMessage);
    if (LOGGER.isDebug()) {
      LOGGER.debug(">> addResponse [callID=%s size=%d replyTo=%s]", callID, messageBytes.length, replyTo);
    }
  }

  private void sendResponseFragments(byte[] messageBytes) throws JMSException, IOException {
    UUID responseID = UUID.randomUUID();
    try (InputStream messageDataStream = new ByteArrayInputStream(messageBytes)) {

      fragment(messageDataStream, maxMessageSize, new FragmentConsumer() {
        @Override
        public void fragment(byte[] data, int idx) throws JMSException {
          BytesMessage fragment = createByteMessage(session, data, protocolVersion, serializer.serializerID());
          fragment.setJMSCorrelationID(callID);
          fragment.setJMSReplyTo(acknowledgementTo);
          fragment.setStringProperty(PROPERTY_SERVER_NODE_ID, serverNodeID.toString());
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
          javax.jms.Message eof = createTextMessage(session, "End-Of-Stream", protocolVersion);
          eof.setJMSCorrelationID(callID);
          eof.setJMSReplyTo(acknowledgementTo);
          eof.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_END_OF_FRAGMENTED_MESSAGE);
          eof.setStringProperty(PROPERTY_RESPONSE_ID, responseID.toString());
          //send total number of fragments and message digest with EOS message, to allow receiver to verify
          eof.setIntProperty(PROPERTY_FRAGMENTS_TOTAL, fragments);
          eof.setStringProperty(PROPERTY_DATA_CHECKSUM_MD5, hex(digest));
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

  public boolean isClosed() {
    if (closed.get()) {
      return true;
    } else if (clock.millis() > timeout.get()) {
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

  @Override
  public void notifyClose() {
    close();
  }

  public void notifyError(Throwable e) {
    if (!isClosed()) {
      try {
        ExceptionMessage ex = new ExceptionMessage(callID, e);
        javax.jms.Message exMessage = createByteMessage(session, serializer.serialize(ex), protocolVersion, serializer.serializerID());
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
        javax.jms.Message closeMessage = createTextMessage(session, "stream closed", protocolVersion);
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

  @Override
  public void acknowledgeResponse() {
    if (LOGGER.isDebug()) {
      LOGGER.debug(" << responseAcknowledgement releasing segment window (available=%d)", segmentWindow.availablePermits());
    }
    segmentWindow.release();
    ifNotNullDo(lastResponseListener.get(), ResponseListener::responseAccepted);
  }

  //builder

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    //fields
    private UUID serverNodeID;
    private String callID;
    private Session session;
    private MessageProducer replyProducer;
    private Destination replyTo;
    private Destination acknowledgementTo;
    private long timeout;
    private ProtocolVersion protocolVersion = ProtocolVersion.V3;
    private int maxMessageSize = DEFAULT_MAX_MAX_MESSAGE_SIZE;
    private int segmentWindowSize = DEFAULT_SEGMENT_WINDOW_SIZE;
    private int maxWindowWaitSeconds = DEFAULT_WINDOW_WAIT_SECONDS;
    private int maxWindowAttempts = DEFAULT_MAX_WINDOW_ATTEMPTS;
    private ServerMetrics metrics;
    private MessageSerializer serializer;
    private RequestSink requestSink;

    public ServerResponseContext build() {
      return new ServerResponseContext(
          callID,
          session,
          replyProducer,
          replyTo,
          acknowledgementTo,
          timeout,
          serverNodeID, protocolVersion,
          maxMessageSize,
          maxWindowAttempts,
          maxWindowWaitSeconds,
          segmentWindowSize,
          metrics,
          serializer,
          requestSink
      );
    }

    //setters


    public Builder setServerNodeID(UUID serverNodeID) {
      this.serverNodeID = serverNodeID;
      return this;
    }

    public Builder setMaxWindowWaitSeconds(int maxWindowWaitSeconds) {
      this.maxWindowWaitSeconds = maxWindowWaitSeconds;
      return this;
    }

    public Builder setMaxWindowAttempts(int maxWindowAttempts) {
      this.maxWindowAttempts = maxWindowAttempts;
      return this;
    }

    public Builder setCallID(String callID) {
      this.callID = callID;
      return this;
    }

    public Builder setSession(Session session) {
      this.session = session;
      return this;
    }

    public Builder setReplyProducer(MessageProducer replyProducer) {
      this.replyProducer = replyProducer;
      return this;
    }

    public Builder setReplyTo(Destination replyTo) {
      this.replyTo = replyTo;
      return this;
    }

    public Builder setAcknowledgementTo(Destination acknowledgementTo) {
      this.acknowledgementTo = acknowledgementTo;
      return this;
    }

    public Builder setTimeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder setProtocolVersion(ProtocolVersion protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
    }

    public Builder setMaxMessageSize(int maxMessageSize) {
      this.maxMessageSize = maxMessageSize;
      return this;
    }

    public Builder setSegmentWindowSize(int segmentWindowSize) {
      this.segmentWindowSize = segmentWindowSize;
      return this;
    }

    public Builder setMetrics(ServerMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    public Builder setSerializer(MessageSerializer serializer) {
      this.serializer = serializer;
      return this;
    }

    public Builder setRequestSink(RequestSink requestSink) {
      this.requestSink = requestSink;
      return this;
    }
  }


  //for testing only

  static void setClock(Clock clock) {
    ServerResponseContext.clock = clock;
  }

  int getAvailableSegmentWindow() {
    return segmentWindow.availablePermits();
  }
}
