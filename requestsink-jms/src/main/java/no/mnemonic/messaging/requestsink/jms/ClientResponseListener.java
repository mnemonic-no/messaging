package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.ClassLoaderContext;
import no.mnemonic.messaging.requestsink.MessagingException;

import javax.jms.*;
import java.util.Date;
import java.util.Objects;

import static no.mnemonic.messaging.requestsink.jms.JMSRequestProxy.*;

/**
 * This listener listens for incoming reply messages and adds them to the correct responsehandler
 *
 * @author joakim
 */
class ClientResponseListener implements javax.jms.MessageListener {

  private static final Logger LOGGER = Logging.getLogger(ClientResponseListener.class);
  private static final String RECEIVED_FRAGMENT_WITHOUT = "Received fragment without ";
  private static final String RECEIVED_END_OF_FRAGMENTS_WITHOUT = "Received end-of-fragments without ";

  private final Session session;
  private final TemporaryQueue responseQueue;
  private final MessageConsumer responseConsumer;
  private final ClientRequestState requestState;
  private final String callID;

  ClientResponseListener(String callID, Session session, ClientRequestState requestState) {
    if (callID == null) throw new IllegalArgumentException("callID not set");
    if (session == null) throw new IllegalArgumentException("session not set");
    if (requestState == null) throw new IllegalArgumentException("requestState not set");
    try {
      this.callID = callID;
      this.session = session;
      this.requestState = requestState;
      responseQueue = session.createTemporaryQueue();
      responseConsumer = session.createConsumer(responseQueue);
      responseConsumer.setMessageListener(this);
    } catch (Exception e) {
      LOGGER.warning(e, "Error setting up response listener");
      throw new MessagingException("Error setting up response listener", e);
    }
  }

  TemporaryQueue getResponseQueue() {
    return responseQueue;
  }

  public void onMessage(javax.jms.Message message) {
    try {
      if (!JMSUtils.isCompatible(message)) {
        LOGGER.warning("Ignoring message of incompatible version");
        return;
      }
      handleResponse(message);
    } catch (Exception e) {
      LOGGER.error(e, "Error receiving message");
    }
  }

  void close() {
    try {
      JMSUtils.removeMessageListenerAndClose(responseConsumer);
      JMSUtils.deleteTemporaryQueue(responseQueue);
    } catch (Exception e) {
      LOGGER.warning(e, "Error in close()");
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  private boolean handleResponse(javax.jms.Message message) throws JMSException {
    if (message == null) {
      LOGGER.warning("No message");
      return false;
    }
    if (message.getJMSCorrelationID() == null) {
      LOGGER.warning("Message received without callID");
      return false;
    }
    if (!Objects.equals(callID, message.getJMSCorrelationID())) {
      LOGGER.warning("Message received with wrong callID: %s (type=%s)",
              message.getJMSCorrelationID(),
              message.getStringProperty(PROPERTY_MESSAGE_TYPE));
      return false;
    }
    if (requestState.getRequestContext().isClosed()) {
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
        LOGGER.warning("Ignoring invalid response type: " + responseType);
        return false;
    }
  }

  private boolean handleSignalResponseFragment(javax.jms.Message fragmentSignal) throws JMSException {
    if (fragmentSignal == null) throw new IllegalArgumentException("fragment was null");
    if (!fragmentSignal.propertyExists(JMSBase.PROPERTY_RESPONSE_ID)) {
      LOGGER.warning(RECEIVED_FRAGMENT_WITHOUT + JMSBase.PROPERTY_RESPONSE_ID);
      return false;
    }
    if (!fragmentSignal.propertyExists(JMSBase.PROPERTY_FRAGMENTS_IDX)) {
      LOGGER.warning(RECEIVED_FRAGMENT_WITHOUT + JMSBase.PROPERTY_FRAGMENTS_IDX);
      return false;
    }
    MessageFragment messageFragment = new MessageFragment((BytesMessage) fragmentSignal);
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< addFragment [callID=%s responseID=%s idx=%d size=%d]",
              messageFragment.getCallID(), messageFragment.getResponseID(),
              messageFragment.getIdx(), messageFragment.getData().length);
    }
    return requestState.addFragment(messageFragment);
  }

  private boolean handleEndOfFragmentedResponse(Message endMessage) throws JMSException {
    if (endMessage == null) throw new IllegalArgumentException("end-message was null");
    if (!endMessage.propertyExists(JMSBase.PROPERTY_RESPONSE_ID)) {
      LOGGER.warning(RECEIVED_END_OF_FRAGMENTS_WITHOUT + JMSBase.PROPERTY_RESPONSE_ID);
      return false;
    }
    if (!endMessage.propertyExists(JMSBase.PROPERTY_FRAGMENTS_TOTAL)) {
      LOGGER.warning(RECEIVED_END_OF_FRAGMENTS_WITHOUT + JMSBase.PROPERTY_FRAGMENTS_TOTAL);
      return false;
    }
    if (!endMessage.propertyExists(JMSBase.PROPERTY_DATA_CHECKSUM_MD5)) {
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
    return requestState.reassembleFragments(responseID, totalFragments, checksum);
  }

  private boolean handleSignalResponse(Message response) throws JMSException {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< addResponse [callID=%s]", response.getJMSCorrelationID());
    }
    try (ClassLoaderContext ignored = ClassLoaderContext.of(requestState.getClassLoader())) {
      return requestState.getRequestContext().addResponse(JMSUtils.extractObject(response));
    }
  }

  private boolean handleChannelSetup(Message response) throws JMSException {
    try {
      if (requestState.getRequestContext() instanceof ChannelUploadMessageContext) {
        if (LOGGER.isDebug()) {
          LOGGER.debug("<< uploadChannel [callID=%s uploadChannel=%s]", response.getJMSCorrelationID(), response.getJMSReplyTo());
        }
        ChannelUploadMessageContext uploadCtx = (ChannelUploadMessageContext) requestState.getRequestContext();
        uploadCtx.upload(session, response.getJMSReplyTo());
        return true;
      } else {
        LOGGER.warning("Received channel setup for a non-channel-upload client context: " + response.getJMSCorrelationID());
        return false;
      }
    } catch (Exception e) {
      LOGGER.warning(e, "Error in handleChannelSetup");
      requestState.getRequestContext().notifyError(e);
      return false;
    }
  }

  private boolean handleSignalEndOfStream(Message response) throws JMSException {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< endOfStream [callID=%s]", response.getJMSCorrelationID());
    }
    requestState.getRequestContext().endOfStream();
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
    requestState.getRequestContext().keepAlive(timeout);
    return true;
  }

  private boolean handleSignalError(Message response) throws JMSException {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< signalError [callID=%s]", response.getJMSCorrelationID());
    }
    if (MESSAGE_TYPE_STREAM_CLOSED.equals(response.getStringProperty(PROPERTY_MESSAGE_TYPE))) {
      requestState.getRequestContext().endOfStream();
    } else {
      try (ClassLoaderContext ignored = ClassLoaderContext.of(requestState.getClassLoader())) {
        ExceptionMessage em = JMSUtils.extractObject(response);
        //noinspection ThrowableResultOfMethodCallIgnored
        requestState.getRequestContext().notifyError(em.getException());
      }
    }
    return true;
  }

}
