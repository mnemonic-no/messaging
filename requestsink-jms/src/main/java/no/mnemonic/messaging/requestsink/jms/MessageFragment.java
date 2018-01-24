package no.mnemonic.messaging.requestsink.jms;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import static no.mnemonic.messaging.requestsink.jms.JMSRequestProxy.PROPERTY_FRAGMENTS_IDX;

class MessageFragment {

  private final int idx;
  private final byte[] data;

  MessageFragment(BytesMessage message) throws JMSException {
    if (message == null) throw new IllegalArgumentException("message was null");
    this.idx = message.getIntProperty(PROPERTY_FRAGMENTS_IDX);
    this.data = new byte[(int) message.getBodyLength()];
    message.readBytes(data);
  }

  int getIdx() {
    return idx;
  }

  byte[] getData() {
    return data;
  }
}
