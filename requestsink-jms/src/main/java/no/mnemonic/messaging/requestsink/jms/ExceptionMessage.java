package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.messaging.requestsink.Message;

public class ExceptionMessage implements Message {

  private static final long serialVersionUID = 5395948817423058966L;

  private String callID;
  private Throwable exception;
  private long timestamp;

  ExceptionMessage(String callID, Throwable exception) {
    this.callID = callID;
    this.exception = exception;
    this.timestamp = System.currentTimeMillis();
  }

  @Override
  public String getCallID() {
    return callID;
  }

  @Override
  public long getMessageTimestamp() {
    return timestamp;
  }

  public Throwable getException() {
    return exception;
  }
}
