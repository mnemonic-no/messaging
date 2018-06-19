package no.mnemonic.messaging.requestsink.jms.serializer.packagea;

import java.util.Objects;

public class MyInnerClass extends MyAbstractClass {
  public String value;

  public MyInnerClass(long abstractValue, String value) {
    super(abstractValue);
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    MyInnerClass that = (MyInnerClass) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), value);
  }
}
