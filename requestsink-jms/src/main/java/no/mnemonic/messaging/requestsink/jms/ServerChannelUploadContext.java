package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;

import javax.jms.*;
import javax.naming.NamingException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static no.mnemonic.messaging.requestsink.jms.JMSUtils.assertNotNull;

/**
 * This context handles fragmented uploading of the signal message on the JMSRequestProxy (server) side.
 * <ul>
 * <li>Set up temporary upload channel</li>
 * <li>Signal client with upload channel</li>
 * <li>Accept fragments and end-of-stream from client</li>
 * <li>Reassemble fragments, verify and submit reassembled message to RequestSink</li>
 * </ul>
 */
class ServerChannelUploadContext implements JMSRequestProxy.ServerContext {

  private static final Logger LOGGER = Logging.getLogger(ServerChannelUploadContext.class);

  private final String callID;
  private final Session session;
  private final Destination responseDestination;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final BlockingQueue<MessageFragment> fragments = new LinkedBlockingDeque<>();
  private final AtomicLong timeout = new AtomicLong();
  private final ProtocolVersion protocolVersion;

  private UploadHandler uploadHandler;
  private MessageProducer replyTo;
  private TemporaryQueue channelQueue;
  private MessageConsumer channelConsumer;

  ServerChannelUploadContext(String callID, Session session, Destination responseDestination, long timeout, ProtocolVersion protocolVersion) throws JMSException, NamingException {
    this.callID = assertNotNull(callID, "CallID not set");
    this.session = assertNotNull(session, "Session not set");
    this.responseDestination = assertNotNull(responseDestination, "ResponseDestination not set");
    this.protocolVersion = assertNotNull(protocolVersion, "ProtocolVersion not set");
    this.timeout.set(timeout);
  }

  /**
   * @param handler handler to receive the reassembled uploaded message
   * @throws JMSException on error receiving from JMS
   */
  void setupChannel(UploadHandler handler) throws JMSException {
    //save reference to handler which should get the reassembled message
    this.uploadHandler = assertNotNull(handler, "UploadHandler not set");
    //create a temporary upload queue, a consumer on that queue, and bind a messagelistener to it
    this.channelQueue = session.createTemporaryQueue();
    this.channelConsumer = session.createConsumer(channelQueue);
    this.channelConsumer.setMessageListener(this::onMessage);
    //create producer to send feedback to the client
    this.replyTo = session.createProducer(responseDestination);
    //send a channel setup message to the client (message text has no meaning)
    Message setupMessage = JMSUtils.createTextMessage(session, "channel setup", protocolVersion);
    setupMessage.setJMSCorrelationID(callID);
    setupMessage.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_CHANNEL_SETUP);
    setupMessage.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, timeout.get());
    setupMessage.setJMSReplyTo(channelQueue);
    replyTo.send(setupMessage);
  }

  //private methods

  private void onMessage(Message message) {
    try {
      if (!JMSUtils.isCompatible(message)) {
        LOGGER.warning("Ignoring incompatible message: " + message);
        return;
      }
      String messageType = message.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE);
      if (JMSRequestProxy.MESSAGE_TYPE_EXCEPTION.equals(messageType)) {
        abortUpload();
      } else if (JMSRequestProxy.MESSAGE_TYPE_SIGNAL_FRAGMENT.equals(messageType)) {
        handleUploadFragment(message);
      } else if (JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED.equals(messageType)) {
        handleSignalEndOfStream(message);
      } else if (JMSRequestProxy.MESSAGE_TYPE_EXTEND_WAIT.equals(messageType)) {
        handleSignalExtendWait(message);
      } else {
        LOGGER.warning("Ignoring invalid channel message type: " + messageType);
      }
    } catch (Throwable e) {
      LOGGER.error(e, "Error receiving message");
    }
  }

  private void handleUploadFragment(Message message) throws JMSException {
    if (LOGGER.isDebug()) LOGGER.debug("ServerChannelUploadContext.handleUploadFragment");
    String callID = message.getJMSCorrelationID();
    if (!callID.equals(this.callID)) {
      LOGGER.warning("Ignoring fragment with wrong callID: " + callID);
      return;
    }
    //extend timeout if client is requesting timeout extention
    long reqTimeout = message.getLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT);
    timeout.updateAndGet(v -> v < reqTimeout ? reqTimeout : v);
    fragments.add(new MessageFragment((BytesMessage) message));
  }

  private void handleSignalEndOfStream(Message message) {
    if (LOGGER.isDebug()) LOGGER.debug("ServerChannelUploadContext.handleSignalEndOfStream");
    close();
    try {
      byte[] messageData = prepareData(message);
      if (messageData == null) {
        LOGGER.warning("Ignoring empty channel upload: " + callID);
        return;
      }
      uploadHandler.handleRequest(callID, messageData, responseDestination, timeout.get(), protocolVersion);
    } catch (Exception e) {
      notifyError(e);
    }
  }

  private void handleSignalExtendWait(Message msg) {
    LOGGER.info("Unexpected message: ServerChannelUploadContext.handleSignalExtendWait");
  }

  private void abortUpload() {
    LOGGER.info("Unexpected message: ServerChannelUploadContext.abortUpload");
    close();
  }

  private byte[] prepareData(Message eosMessage) throws Exception {
    int expectedFragments = eosMessage.getIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_TOTAL);
    String transmittedChecksum = eosMessage.getStringProperty(JMSRequestProxy.PROPERTY_DATA_CHECKSUM_MD5);
    return JMSUtils.reassembleFragments(fragments, expectedFragments, transmittedChecksum);
  }

  private void notifyError(Throwable e) {
    try {
      ExceptionMessage ex = new ExceptionMessage(callID, e);
      javax.jms.Message exMessage;
      exMessage = JMSUtils.createByteMessage(session, JMSUtils.serialize(ex), protocolVersion);
      exMessage.setJMSCorrelationID(callID);
      exMessage.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_EXCEPTION);
      replyTo.send(exMessage);
    } catch (Exception e1) {
      LOGGER.warning("Could not send error notification for " + callID);
      close();
    }
  }

  public boolean isClosed() {
    if (closed.get()) return true;
    if (System.currentTimeMillis() > timeout.get()) {
      close();
      return true;
    }
    return false;
  }

  private void close() {
    closed.set(true);
    JMSUtils.removeMessageListenerAndClose(channelConsumer);
    JMSUtils.deleteTemporaryQueue(channelQueue);
  }

  public interface UploadHandler {
    void handleRequest(String callID, byte[] message, Destination replyTo, long timeout, ProtocolVersion protocolVersion) throws Exception;
  }
}
