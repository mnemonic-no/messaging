package no.mnemonic.messaging.requestsink.jms.serializer.packagea;

import no.mnemonic.messaging.requestsink.Message;

import java.util.Objects;

public class MyComplexMessage implements Message {
  private MyInnerClass innerClass;
  public MyComplexMessage(MyInnerClass innerClass) {
    this.innerClass = innerClass;
  }
  @Override
  public String getCallID() {return null;}
  @Override
  public long getMessageTimestamp() {return 0;}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MyComplexMessage that = (MyComplexMessage) o;
    return Objects.equals(innerClass, that.innerClass);
  }

  @Override
  public int hashCode() {
    return Objects.hash(innerClass);
  }
}
