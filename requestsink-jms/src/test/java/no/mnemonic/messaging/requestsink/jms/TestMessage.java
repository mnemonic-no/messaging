package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.messaging.requestsink.Message;

import java.util.Objects;
import java.util.UUID;

public class TestMessage implements Message {

  private static final long serialVersionUID = -8911834432780676086L;
  
  private final String id;
  private String callID;
  private long timestamp = System.currentTimeMillis();

  public TestMessage(String id) {
    this.id = id;
    this.callID = UUID.randomUUID().toString();
  }

  @Override
  public long getMessageTimestamp() {
    return timestamp;
  }

  @Override
  public String getCallID() {
    return callID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TestMessage that = (TestMessage) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  public String getId() {
    return id;
  }
}
