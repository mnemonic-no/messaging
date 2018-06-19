package no.mnemonic.messaging.requestsink.jms.serializer.packagea;

import java.util.Objects;

public class MyAbstractClass {
  private long abstractValue;

  public MyAbstractClass() {
  }

  public MyAbstractClass(long abstractValue) {
    this.abstractValue = abstractValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MyAbstractClass that = (MyAbstractClass) o;
    return abstractValue == that.abstractValue;
  }

  @Override
  public int hashCode() {
    return Objects.hash(abstractValue);
  }
}
