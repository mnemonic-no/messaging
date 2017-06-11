package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestListener;

import javax.jms.*;
import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Clock;

import static no.mnemonic.messaging.requestsink.jms.JMSUtils.assertNotNull;

/**
 * This context handles fragmented upload of the signal message on the JMSRequestSink (client) side
 * <ul>
 *   <li>Send a channel request message to the server, and wait for reply</li>
 *   <li>Fragment the message into suitable fragments, and submit to upload channel</li>
 *   <li>Finish stream with end-of-stream, and close the upload channel</li>
 * </ul>
 */
class ChannelUploadMessageContext implements RequestContext {

  private static final Logger LOGGER = Logging.getLogger(ServerResponseContext.class);
  private static final int KEEPALIVE_PERIOD = 10000;
  private static Clock clock = Clock.systemUTC();

  private final RequestContext realContext;
  private final InputStream messageData;
  private final String callID;
  private final int fragmentSize;
  private final ProtocolVersion protocolVersion;

  ChannelUploadMessageContext(RequestContext realContext, InputStream messageData, String callID, int fragmentSize, ProtocolVersion protocolVersion) {
    this.realContext = assertNotNull(realContext, "RequestContext not set");
    this.messageData = assertNotNull(messageData, "Message data not set");
    this.callID = assertNotNull(callID, "CallID not set");
    this.protocolVersion = assertNotNull(protocolVersion, "ProtocolVersion not set");
    if (fragmentSize <= 0) throw new IllegalArgumentException("FragmentSize must be a positive integer");
    this.fragmentSize = fragmentSize;
  }

  void upload(Session session, Destination uploadChannel) throws JMSException {
    assertNotNull(session, "Session not provided");
    assertNotNull(uploadChannel, "UploadChannel not provided");
    try {
      if (LOGGER.isDebug()) {
        LOGGER.debug(String.format("Initializing channel upload for callID %s to destination %s", callID, uploadChannel));
      }
      //create producer to send fragments
      MessageProducer producer = session.createProducer(uploadChannel);
      try {
        //create buffer for fragments
        byte[] bytes = new byte[fragmentSize];
        int size;
        int fragmentIndex = 0;
        //create a MD5 digester to calculate a checksum
        MessageDigest digester = JMSUtils.md5();

        //read each fragment
        while ((size = messageData.read(bytes)) >= 0) {
          digester.update(bytes, 0, size);
          //create fragment message with correlationID, messagetype, fragment index
          BytesMessage fragment = JMSUtils.createByteMessage(session, JMSUtils.arraySubSeq(bytes, 0, size), protocolVersion);
          fragment.setJMSCorrelationID(callID);
          fragment.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_SIGNAL_FRAGMENT);
          fragment.setIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_IDX, fragmentIndex++);
          fragment.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, clock.millis() + KEEPALIVE_PERIOD);
          //send fragment to upload channel
          producer.send(uploadChannel, fragment);
        }
        //prepare EOS message (message text has no meaning)
        javax.jms.Message eos = JMSUtils.createTextMessage(session, "End-Of-Stream", protocolVersion);
        eos.setJMSCorrelationID(callID);
        eos.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED);
        //send total number of fragments and message digest with EOS message, to allow receiver to verify
        eos.setIntProperty(JMSRequestProxy.PROPERTY_FRAGMENTS_TOTAL, fragmentIndex);
        eos.setStringProperty(JMSRequestProxy.PROPERTY_DATA_CHECKSUM_MD5, JMSUtils.hex(digester.digest()));
        //send EOS
        producer.send(uploadChannel, eos);
        if (LOGGER.isDebug()) {
          LOGGER.debug(String.format("Completed sending %d fragments for callID %s", fragmentIndex, callID));
        }
      } finally {
        producer.close();
      }
    } catch (Exception e) {
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
