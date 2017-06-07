package no.mnemonic.messaging.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.ByteArrayOutputStream;
import java.lang.IllegalStateException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static no.mnemonic.messaging.jms.JMSRequestProxy.*;

/**
 * This listener listens for incoming reply messages and adds them to the correct responsehandler
 *
 * @author joakim
 */
class ServerChannelUploadContext implements JMSRequestProxy.ServerContext {

  private static final Logger LOGGER = Logging.getLogger(ServerChannelUploadContext.class);

  private final String callID;
  private final Session session;
  private final Destination responseDestination;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final BlockingQueue<Message> fragments = new LinkedBlockingDeque<>();
  private final AtomicLong timeout = new AtomicLong();
  private final ProtocolVersion protocolVersion;

  private UploadHandler uploadHandler;
  private MessageProducer replyTo;
  private TemporaryQueue channelQueue;
  private MessageConsumer channelConsumer;

  ServerChannelUploadContext(String callID, Session session, Destination responseDestination, long timeout, ProtocolVersion protocolVersion) throws JMSException, NamingException {
    this.callID = callID;
    this.session = session;
    this.responseDestination = responseDestination;
    this.protocolVersion = protocolVersion;
    this.timeout.set(timeout);
  }

  void setupChannel(UploadHandler handler) throws JMSException {
    this.uploadHandler = handler;
    this.channelQueue = session.createTemporaryQueue();
    this.channelConsumer = session.createConsumer(channelQueue);
    this.channelConsumer.setMessageListener(this::onMessage);

    this.replyTo = session.createProducer(responseDestination);
    Message setupMessage = JMSUtils.createTextMessage(session, "channel setup", ProtocolVersion.V1);
    setupMessage.setJMSCorrelationID(callID);
    setupMessage.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_CHANNEL_SETUP);
    setupMessage.setLongProperty(PROPERTY_REQ_TIMEOUT, timeout.get());
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
      String messageType = message.getStringProperty(PROPERTY_MESSAGE_TYPE);
      if (MESSAGE_TYPE_EXCEPTION.equals(messageType)) {
        abortUpload();
      } else if (MESSAGE_TYPE_SIGNAL_FRAGMENT.equals(messageType)) {
        handleUploadFragment(message);
      } else if (MESSAGE_TYPE_STREAM_CLOSED.equals(messageType)) {
        handleSignalEndOfStream(message);
      } else if (MESSAGE_TYPE_EXTEND_WAIT.equals(messageType)) {
        handleSignalExtendWait(message);
      } else {
        LOGGER.warning("Ignoring invalid channel message type: " + messageType);
      }
    } catch (Throwable e) {
      LOGGER.error(e, "Error receiving message");
    }
  }

  private void handleUploadFragment(Message message) throws JMSException {
    if (LOGGER.isDebug()) LOGGER.debug("UploadListener.handleUploadFragment");
    String callID = message.getJMSCorrelationID();
    if (!callID.equals(this.callID)) {
      LOGGER.warning("Ignoring fragment with wrong callID: " + callID);
      return;
    }
    //extend timeout if client is requesting timeout extention
    long reqTimeout = message.getLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT);
    timeout.updateAndGet(v->v < reqTimeout ? reqTimeout : v);
    fragments.add(message);
  }

  private void handleSignalEndOfStream(Message message) {
    if (LOGGER.isDebug()) LOGGER.debug("UploadListener.handleSignalEndOfStream");
    close();
    try {
      byte[] messageData = prepareData(message);
      if (messageData == null) {
        LOGGER.warning("Ignoring empty channel upload: " + callID);
        return;
      }
      uploadHandler.handleRequest(callID, messageData, responseDestination, timeout.get());
    } catch (Exception e) {
      notifyError(e);
    }
  }

  private void handleSignalExtendWait(Message msg) {
    if (LOGGER.isDebug()) LOGGER.debug("UploadListener.handleSignalExtendWait");
  }

  private void abortUpload() {
    if (LOGGER.isDebug()) LOGGER.debug("UploadListener.abortUpload");
    close();
  }

  private byte[] prepareData(Message eosMessage) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int expectedFragments = eosMessage.getIntProperty(PROPERTY_FRAGMENTS_TOTAL);
    String transmittedChecksum = eosMessage.getStringProperty(PROPERTY_DATA_CHECKSUM_MD5);

    if (fragments.size() != expectedFragments) {
      throw new IllegalStateException(String.format("Expected %d fragments, received %d", expectedFragments, fragments.size()));
    }

    //sort fragments in case they are out-of-order
    List<Message> fragmentList = new ArrayList<>();
    fragments.drainTo(fragmentList);
    fragmentList.sort(Comparator.comparing(m -> {
      try {
        return m.getIntProperty(PROPERTY_FRAGMENTS_IDX);
      } catch (JMSException e) {
        throw new RuntimeException(e);
      }
    }));

    int fragmentIndex = 0;
    MessageDigest digest = JMSUtils.md5();
    for (Message m : fragmentList) {
      //verify that fragment makes sense
      if (fragmentIndex != m.getIntProperty(PROPERTY_FRAGMENTS_IDX))
        throw new IllegalStateException("Expected fragment index " + fragmentIndex);
      //extract data and write to BAOS
      BytesMessage bytesMessage = (BytesMessage) m;
      byte[] data = new byte[(int) bytesMessage.getBodyLength()];
      bytesMessage.readBytes(data);
      baos.write(data);
      digest.update(data);
      //increment expected fragment index
      fragmentIndex++;
    }
    baos.close();

    //verify checksum
    String computedChecksum = JMSUtils.hex(digest.digest());
    if (!computedChecksum.equals(transmittedChecksum)) {
      throw new IllegalStateException("Data checksum mismatch");
    }

    return baos.toByteArray();
  }

  private void notifyError(Throwable e) {
    try {
      ExceptionMessage ex = new ExceptionMessage(callID, e);
      javax.jms.Message exMessage;
      exMessage = JMSUtils.createByteMessage(session, JMSUtils.serialize(ex), protocolVersion);
      exMessage.setJMSCorrelationID(callID);
      exMessage.setStringProperty(PROPERTY_MESSAGE_TYPE, MESSAGE_TYPE_EXCEPTION);
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
    void handleRequest(String callID, byte[] message, Destination replyTo, long timeout) throws Exception;
  }
}
