package no.mnemonic.messaging.jms;

import no.mnemonic.messaging.api.Message;

public class ExceptionMessage implements Message {

  private static final long serialVersionUID = 5395948817423058966L;

  private String callID;
  private Throwable exception;
  private long timestamp;

  public ExceptionMessage(Throwable exception) {
    this.exception = exception;
    this.timestamp = System.currentTimeMillis();
  }

  @Override
  public String getCallID() {
    return callID;
  }

  @Override
  public void setCallID(String callID) {
    this.callID = callID;
  }

  @Override
  public long getMessageTimestamp() {
    return timestamp;
  }

  public Throwable getException() {
    return exception;
  }
}
