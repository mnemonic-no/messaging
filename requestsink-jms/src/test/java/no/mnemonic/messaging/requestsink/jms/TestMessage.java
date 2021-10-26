package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.messaging.requestsink.Message;

import java.util.Objects;
import java.util.UUID;

public class TestMessage implements Message {

  private static final long serialVersionUID = -8911834432780676086L;

  public enum MyEnum {
    literal1, literal2
  }

  private final String id;
  private String callID;
  private long timestamp = System.currentTimeMillis();
  public SubClass objectField1;
  public SubClass objectField2;
  public MyEnum enumField;

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

  public static class SubClass {
    public final String field;

    public SubClass(String field) {
      this.field = field;
    }
  }
}
