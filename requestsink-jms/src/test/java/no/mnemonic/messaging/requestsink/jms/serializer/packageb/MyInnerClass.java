package no.mnemonic.messaging.requestsink.jms.serializer.packageb;

import java.util.Objects;

public class MyInnerClass {
  public String value;

  public MyInnerClass(String value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MyInnerClass that = (MyInnerClass) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
