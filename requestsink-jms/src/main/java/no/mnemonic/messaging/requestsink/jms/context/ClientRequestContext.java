package no.mnemonic.messaging.requestsink.jms.context;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.ClassLoaderContext;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.jms.AbstractJMSRequestBase;
import no.mnemonic.messaging.requestsink.jms.ExceptionMessage;
import no.mnemonic.messaging.requestsink.jms.serializer.MessageSerializer;
import no.mnemonic.messaging.requestsink.jms.util.ClientMetrics;
import no.mnemonic.messaging.requestsink.jms.util.MessageFragment;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import static no.mnemonic.commons.utilities.collections.CollectionUtils.isEmpty;
import static no.mnemonic.messaging.requestsink.jms.JMSRequestProxy.*;
import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.*;

/**
 * This listener listens for incoming reply messages and adds them to the correct responsehandler
 *
 * @author joakim
 */
public class ClientRequestContext {

  private static final Logger LOGGER = Logging.getLogger(ClientRequestContext.class);
  private static final String RECEIVED_FRAGMENT_WITHOUT = "Received fragment without ";
  private static final String RECEIVED_END_OF_FRAGMENTS_WITHOUT = "Received end-of-fragments without ";
  private static final long KEEPALIVE_ON_FRAGMENT = 1000;

  private static Clock clock = Clock.systemUTC();

  private final Session session;
  private final String callID;
  private final ClientMetrics metrics;
  private final ClassLoader classLoader;
  private final RequestContext requestContext;
  private final Runnable closeListener;
  private final MessageSerializer serializer;

  private final Map<String, Collection<MessageFragment>> fragments = new ConcurrentHashMap<>();

  public ClientRequestContext(String callID, Session session, ClientMetrics metrics, ClassLoader classLoader,
                       RequestContext requestContext, Runnable closeListener, MessageSerializer serializer) {
    this.serializer = assertNotNull(serializer, "serializer not set");
    this.closeListener = assertNotNull(closeListener, "closeListener not set");
    this.classLoader = assertNotNull(classLoader, "classLoader not set");
    this.requestContext = assertNotNull(requestContext, "requestContext not set");
    this.metrics = assertNotNull(metrics, "metrics not set");
    this.callID = assertNotNull(callID, "callID not set");
    this.session = assertNotNull(session, "session not set");
  }

  public String getCallID() {
    return callID;
  }

  public boolean isClosed() {
    return requestContext.isClosed();
  }

  public void cleanup() {
    closeListener.run();
    requestContext.notifyClose();
  }

  boolean addFragment(MessageFragment messageFragment) {
    if (messageFragment == null) {
      LOGGER.warning("Fragment was null");
      return false;
    }
    fragments
            .computeIfAbsent(messageFragment.getResponseID(), id -> new LinkedBlockingDeque<>())
            .add(messageFragment);
    //notify requestcontext for each fragment to avoid long fragment stream causing timeout
    requestContext.keepAlive(clock.millis() + KEEPALIVE_ON_FRAGMENT);
    return true;
  }

  boolean reassemble(String responseID, int totalFragments, String checksum) {
    try {
      Collection<MessageFragment> responseFragments = this.fragments.remove(responseID);
      if (isEmpty(responseFragments)) {
        LOGGER.warning("Received fragment end-message without preceding fragments");
        return false;
      }
      byte[] reassembledData = reassembleFragments(responseFragments, totalFragments, checksum);
      if (LOGGER.isDebug()) {
        LOGGER.debug("# addReassembledResponse [responseID=%s]", responseID);
      }
      return requestContext.addResponse(serializer.deserialize(reassembledData, classLoader));
    } catch (JMSException | IOException e) {
      LOGGER.warning(e, "Error unpacking fragments");
      requestContext.notifyError(e);
      return false;
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  public boolean handleResponse(javax.jms.Message message) throws JMSException {
    if (message == null) {
      metrics.incompatibleMessage();
      LOGGER.warning("No message");
      return false;
    }
    if (message.getJMSCorrelationID() == null) {
      metrics.incompatibleMessage();
      LOGGER.warning("Message received without callID");
      return false;
    }
    if (!Objects.equals(callID, message.getJMSCorrelationID())) {
      metrics.unknownCallIDMessage();
      LOGGER.warning("Message received with wrong callID: %s (type=%s)",
              message.getJMSCorrelationID(),
              message.getStringProperty(PROPERTY_MESSAGE_TYPE));
      return false;
    }
    if (requestContext.isClosed()) {
      metrics.unknownCallIDMessage();
      LOGGER.warning("Discarding signal response [callID=%s messageType=%s] ",
              message.getJMSCorrelationID(),
              message.getStringProperty(PROPERTY_MESSAGE_TYPE));
      return false;
    }
    String responseType = message.getStringProperty(PROPERTY_MESSAGE_TYPE);
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< handleResponse [callID=%s messageType=%s]", message.getJMSCorrelationID(), responseType);
    }
    if (responseType == null) responseType = "N/A";
    switch (responseType) {
      case MESSAGE_TYPE_SIGNAL_RESPONSE:
        return handleSignalResponse(message);
      case MESSAGE_TYPE_SIGNAL_FRAGMENT:
        return handleSignalResponseFragment(message);
      case MESSAGE_TYPE_END_OF_FRAGMENTED_MESSAGE:
        return handleEndOfFragmentedResponse(message);
      case MESSAGE_TYPE_CHANNEL_SETUP:
        return handleChannelSetup(message);
      case MESSAGE_TYPE_EXCEPTION:
        return handleSignalError(message);
      case MESSAGE_TYPE_STREAM_CLOSED:
        return handleSignalEndOfStream(message);
      case MESSAGE_TYPE_EXTEND_WAIT:
        return handleSignalExtendWait(message);
      default:
        metrics.incompatibleMessage();
        LOGGER.warning("Ignoring invalid response type: " + responseType);
        return false;
    }
  }

  //private methods

  private boolean handleSignalResponseFragment(javax.jms.Message fragmentSignal) throws JMSException {
    if (fragmentSignal == null) throw new IllegalArgumentException("fragment was null");
    if (!fragmentSignal.propertyExists(AbstractJMSRequestBase.PROPERTY_RESPONSE_ID)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_FRAGMENT_WITHOUT + AbstractJMSRequestBase.PROPERTY_RESPONSE_ID);
      return false;
    }
    if (!fragmentSignal.propertyExists(AbstractJMSRequestBase.PROPERTY_FRAGMENTS_IDX)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_FRAGMENT_WITHOUT + AbstractJMSRequestBase.PROPERTY_FRAGMENTS_IDX);
      return false;
    }
    metrics.fragmentedReplyFragment();
    MessageFragment messageFragment = new MessageFragment((BytesMessage) fragmentSignal);
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< addFragment [callID=%s responseID=%s idx=%d size=%d]",
              messageFragment.getCallID(), messageFragment.getResponseID(),
              messageFragment.getIdx(), messageFragment.getData().length);
    }
    return addFragment(messageFragment);
  }

  private boolean handleEndOfFragmentedResponse(Message endMessage) throws JMSException {
    if (endMessage == null) throw new IllegalArgumentException("end-message was null");
    if (!endMessage.propertyExists(AbstractJMSRequestBase.PROPERTY_RESPONSE_ID)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_END_OF_FRAGMENTS_WITHOUT + AbstractJMSRequestBase.PROPERTY_RESPONSE_ID);
      return false;
    }
    if (!endMessage.propertyExists(AbstractJMSRequestBase.PROPERTY_FRAGMENTS_TOTAL)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_END_OF_FRAGMENTS_WITHOUT + AbstractJMSRequestBase.PROPERTY_FRAGMENTS_TOTAL);
      return false;
    }
    if (!endMessage.propertyExists(AbstractJMSRequestBase.PROPERTY_DATA_CHECKSUM_MD5)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_END_OF_FRAGMENTS_WITHOUT + AbstractJMSRequestBase.PROPERTY_DATA_CHECKSUM_MD5);
      return false;
    }
    String responseID = endMessage.getStringProperty(AbstractJMSRequestBase.PROPERTY_RESPONSE_ID);
    int totalFragments = endMessage.getIntProperty(AbstractJMSRequestBase.PROPERTY_FRAGMENTS_TOTAL);
    String checksum = endMessage.getStringProperty(AbstractJMSRequestBase.PROPERTY_DATA_CHECKSUM_MD5);
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< reassemble [callID=%s responseID=%s fragments=%d]",
              callID, responseID, totalFragments);
    }
    metrics.fragmentedReplyCompleted();
    return reassemble(responseID, totalFragments, checksum);
  }

  private boolean handleSignalResponse(Message response) throws JMSException {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< addResponse [callID=%s]", response.getJMSCorrelationID());
    }
    try (ClassLoaderContext ignored = ClassLoaderContext.of(classLoader)) {
      metrics.reply();
      return requestContext.addResponse(serializer.deserialize(extractMessageBytes(response), classLoader));
    } catch (IOException e) {
      LOGGER.error(e, "Error deserializing response");
      requestContext.notifyError(e);
      throw new JMSException(e.getMessage());
    }
  }

  private boolean handleChannelSetup(Message response) throws JMSException {
    try {
      if (requestContext instanceof ChannelUploadMessageContext) {
        if (LOGGER.isDebug()) {
          LOGGER.debug("<< uploadChannel [callID=%s uploadChannel=%s]", response.getJMSCorrelationID(), response.getJMSReplyTo());
        }
        ChannelUploadMessageContext uploadCtx = (ChannelUploadMessageContext) requestContext;
        //perform actual upload to dedicated server channel
        uploadCtx.upload(session, response.getJMSReplyTo());
        metrics.fragmentedUploadCompleted();
        return true;
      } else {
        metrics.incompatibleMessage();
        LOGGER.warning("Received channel setup for a non-channel-upload client context: " + response.getJMSCorrelationID());
        return false;
      }
    } catch (Exception e) {
      metrics.error();
      LOGGER.warning(e, "Error in handleChannelSetup");
      requestContext.notifyError(e);
      return false;
    }
  }

  private boolean handleSignalEndOfStream(Message response) throws JMSException {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< endOfStream [callID=%s]", response.getJMSCorrelationID());
    }
    metrics.endOfStream();
    requestContext.endOfStream();
    return true;
  }

  private boolean handleSignalExtendWait(Message response) throws JMSException {
    if (!response.propertyExists(AbstractJMSRequestBase.PROPERTY_REQ_TIMEOUT)) {
      LOGGER.warning("Received ExtendWait signal without property " + AbstractJMSRequestBase.PROPERTY_REQ_TIMEOUT);
      return false;
    }
    long timeout = response.getLongProperty(PROPERTY_REQ_TIMEOUT);
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< extendWait [callID=%s timeout=%s]", response.getJMSCorrelationID(), new Date(timeout));
    }
    metrics.extendWait();
    requestContext.keepAlive(timeout);
    return true;
  }

  private boolean handleSignalError(Message response) throws JMSException {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< signalError [callID=%s]", response.getJMSCorrelationID());
    }
    metrics.exceptionSignal();
    try (ClassLoaderContext ignored = ClassLoaderContext.of(classLoader)) {
      ExceptionMessage em = serializer.deserialize(extractMessageBytes(response), classLoader);
      //noinspection ThrowableResultOfMethodCallIgnored
      requestContext.notifyError(em.getException());
    } catch (IOException e) {
      LOGGER.error(e, "Error deserializing response");
      requestContext.notifyError(e);
      throw new JMSException(e.getMessage());
    }
    return true;
  }


  static void setClock(Clock clock) {
    ClientRequestContext.clock = clock;
  }
}
