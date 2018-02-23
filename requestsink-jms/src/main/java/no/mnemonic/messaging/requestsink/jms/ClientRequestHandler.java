package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.ClassLoaderContext;
import no.mnemonic.messaging.requestsink.RequestContext;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import static no.mnemonic.commons.utilities.collections.CollectionUtils.isEmpty;
import static no.mnemonic.messaging.requestsink.jms.JMSRequestProxy.*;

/**
 * This listener listens for incoming reply messages and adds them to the correct responsehandler
 *
 * @author joakim
 */
class ClientRequestHandler {

  private static final Logger LOGGER = Logging.getLogger(ClientRequestHandler.class);
  private static final String RECEIVED_FRAGMENT_WITHOUT = "Received fragment without ";
  private static final String RECEIVED_END_OF_FRAGMENTS_WITHOUT = "Received end-of-fragments without ";

  private final Session session;
  private final String callID;
  private final ClientMetrics metrics;
  private final ClassLoader classLoader;
  private final RequestContext requestContext;
  private final Runnable closeListener;

  private final Map<String, Collection<MessageFragment>> fragments = new ConcurrentHashMap<>();

  ClientRequestHandler(String callID, Session session, ClientMetrics metrics, ClassLoader classLoader,
                       RequestContext requestContext, Runnable closeListener) {
    if (callID == null) throw new IllegalArgumentException("callID not set");
    if (session == null) throw new IllegalArgumentException("session not set");
    if (metrics == null) throw new IllegalArgumentException("metrics not set");
    if (classLoader == null) throw new IllegalArgumentException("classLoader not set");
    if (requestContext == null) throw new IllegalArgumentException("requestContext not set");
    if (closeListener == null) throw new IllegalArgumentException("closeListener not set");
    this.closeListener = closeListener;
    this.classLoader = classLoader;
    this.requestContext = requestContext;
    this.metrics = metrics;
    this.callID = callID;
    this.session = session;
  }

  String getCallID() {
    return callID;
  }

  boolean isClosed() {
    return requestContext.isClosed();
  }

  void cleanup() {
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
    return true;
  }

  boolean reassembleFragments(String responseID, int totalFragments, String checksum) {
    try {
      Collection<MessageFragment> responseFragments = this.fragments.remove(responseID);
      if (isEmpty(responseFragments)) {
        LOGGER.warning("Received fragment end-message without preceding fragments");
        return false;
      }
      byte[] reassembledData = JMSUtils.reassembleFragments(responseFragments, totalFragments, checksum);
      if (LOGGER.isDebug()) {
        LOGGER.debug("# addReassembledResponse [responseID=%s]", responseID);
      }
      return requestContext.addResponse(JMSUtils.unserialize(reassembledData, classLoader));
    } catch (JMSException | IOException | ClassNotFoundException e) {
      LOGGER.warning(e, "Error unpacking fragments");
      return false;
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  boolean handleResponse(javax.jms.Message message) throws JMSException {
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

  private boolean handleSignalResponseFragment(javax.jms.Message fragmentSignal) throws JMSException {
    if (fragmentSignal == null) throw new IllegalArgumentException("fragment was null");
    if (!fragmentSignal.propertyExists(JMSBase.PROPERTY_RESPONSE_ID)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_FRAGMENT_WITHOUT + JMSBase.PROPERTY_RESPONSE_ID);
      return false;
    }
    if (!fragmentSignal.propertyExists(JMSBase.PROPERTY_FRAGMENTS_IDX)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_FRAGMENT_WITHOUT + JMSBase.PROPERTY_FRAGMENTS_IDX);
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
    if (!endMessage.propertyExists(JMSBase.PROPERTY_RESPONSE_ID)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_END_OF_FRAGMENTS_WITHOUT + JMSBase.PROPERTY_RESPONSE_ID);
      return false;
    }
    if (!endMessage.propertyExists(JMSBase.PROPERTY_FRAGMENTS_TOTAL)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_END_OF_FRAGMENTS_WITHOUT + JMSBase.PROPERTY_FRAGMENTS_TOTAL);
      return false;
    }
    if (!endMessage.propertyExists(JMSBase.PROPERTY_DATA_CHECKSUM_MD5)) {
      metrics.incompatibleMessage();
      LOGGER.warning(RECEIVED_END_OF_FRAGMENTS_WITHOUT + JMSBase.PROPERTY_DATA_CHECKSUM_MD5);
      return false;
    }
    String responseID = endMessage.getStringProperty(JMSBase.PROPERTY_RESPONSE_ID);
    int totalFragments = endMessage.getIntProperty(JMSBase.PROPERTY_FRAGMENTS_TOTAL);
    String checksum = endMessage.getStringProperty(JMSBase.PROPERTY_DATA_CHECKSUM_MD5);
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< reassembleFragments [callID=%s responseID=%s fragments=%d]",
              callID, responseID, totalFragments);
    }
    metrics.fragmentedReplyCompleted();
    return reassembleFragments(responseID, totalFragments, checksum);
  }

  private boolean handleSignalResponse(Message response) throws JMSException {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< addResponse [callID=%s]", response.getJMSCorrelationID());
    }
    try (ClassLoaderContext ignored = ClassLoaderContext.of(classLoader)) {
      metrics.reply();
      return requestContext.addResponse(JMSUtils.extractObject(response));
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
    if (!response.propertyExists(JMSBase.PROPERTY_REQ_TIMEOUT)) {
      LOGGER.warning("Received ExtendWait signal without property " + JMSBase.PROPERTY_REQ_TIMEOUT);
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
      ExceptionMessage em = JMSUtils.extractObject(response);
      //noinspection ThrowableResultOfMethodCallIgnored
      requestContext.notifyError(em.getException());
    }
    return true;
  }

}
