package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestListener;

import javax.jms.*;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

class ChannelUploadMessageContext implements RequestContext {

  private static final Logger LOGGER = Logging.getLogger(ServerResponseContext.class);

  private final RequestContext realContext;
  private final InputStream messageData;
  private final String callID;
  private final int fragmentSize;
  private final ProtocolVersion protocolVersion;

  ChannelUploadMessageContext(RequestContext realContext, InputStream messageData, String callID, int fragmentSize, ProtocolVersion protocolVersion) {
    this.realContext = realContext;
    this.messageData = messageData;
    this.callID = callID;
    this.fragmentSize = fragmentSize;
    this.protocolVersion = protocolVersion;
  }

  void upload(Session session, Destination channelDestination) throws JMSException {
    try {
      if (LOGGER.isDebug()) LOGGER.debug(String.format("Initializing channel upload for callID %s to destination %s", callID, channelDestination));
      MessageProducer producer = session.createProducer(channelDestination);
      try {
        byte[] bytes = new byte[fragmentSize];
        int size;
        int fragmentIndex = 0;
        MessageDigest digest = JMSUtils.md5();
        while ((size = messageData.read(bytes)) >= 0) {
          digest.update(bytes, 0, size);
          BytesMessage fragment = JMSUtils.createByteMessage(session, JMSUtils.arraySubSeq(bytes, 0, size), protocolVersion);
          fragment.setJMSCorrelationID(callID);
          fragment.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_SIGNAL_FRAGMENT);
          fragment.setIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_IDX, fragmentIndex++);
          fragment.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, System.currentTimeMillis() + 10000);
          producer.send(channelDestination, fragment);
        }
        javax.jms.Message eos = JMSUtils.createTextMessage(session, "End-Of-Stream", ProtocolVersion.V1);
        eos.setJMSCorrelationID(callID);
        eos.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED);
        eos.setIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_TOTAL, fragmentIndex);
        eos.setStringProperty(JMSRequestProxy.PROPERTY_DATA_CHECKSUM_MD5, JMSUtils.hex(digest.digest()));
        producer.send(channelDestination, eos);
        if (LOGGER.isDebug()) LOGGER.debug(String.format("Completed sending %d fragments for callID %s", fragmentIndex, callID));
      } finally {
        producer.close();
      }
    } catch (IOException e) {
      throw new JMSException("Error reading from message data stream: " + e.getMessage());
    }
  }

  @Override
  public boolean addResponse(no.mnemonic.messaging.requestsink.Message msg) {
    return realContext.addResponse(msg);
  }

  @Override
  public void endOfStream() {
    realContext.endOfStream();
  }

  @Override
  public boolean isClosed() {
    return realContext.isClosed();
  }

  @Override
  public boolean keepAlive(long until) {
    return realContext.keepAlive(until);
  }

  @Override
  public void notifyError(Throwable e) {
    realContext.notifyError(e);
  }

  @Override
  public void addListener(RequestListener listener) {
    realContext.addListener(listener);
  }

  @Override
  public void removeListener(RequestListener listener) {
    realContext.removeListener(listener);
  }
}
