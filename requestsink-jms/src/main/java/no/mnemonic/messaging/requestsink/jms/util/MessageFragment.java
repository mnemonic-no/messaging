package no.mnemonic.messaging.requestsink.jms.util;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import static no.mnemonic.messaging.requestsink.jms.JMSBase.PROPERTY_RESPONSE_ID;
import static no.mnemonic.messaging.requestsink.jms.JMSRequestProxy.PROPERTY_FRAGMENTS_IDX;

public class MessageFragment {

  private final String callID;
  private final String responseID;
  private final int idx;
  private final byte[] data;

  public MessageFragment(String callID, String responseID, int idx, byte[] data) {
    this.callID = callID;
    this.responseID = responseID;
    this.idx = idx;
    this.data = data;
  }

  public MessageFragment(BytesMessage message) throws JMSException {
    if (message == null) throw new IllegalArgumentException("message was null");
    this.callID = message.getStringProperty(message.getJMSCorrelationID());
    this.responseID = message.getStringProperty(PROPERTY_RESPONSE_ID);
    this.idx = message.getIntProperty(PROPERTY_FRAGMENTS_IDX);
    this.data = new byte[(int) message.getBodyLength()];
    message.readBytes(data);
  }

  public String getResponseID() {
    return responseID;
  }

  public String getCallID() {
    return callID;
  }

  public int getIdx() {
    return idx;
  }

  public byte[] getData() {
    return data;
  }
}
