package no.mnemonic.messaging.requestsink.jms.serializer;

import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.jms.TestMessage;
import no.mnemonic.messaging.requestsink.jms.serializer.packagea.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class XStreamMessageSerializerTest {

  @Test(expected = IOException.class)
  public void testNonAllowedClassNotDeserialized() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedPackage("org.acme.*")
            .build();
    serializer.deserialize(serializer.serialize(new TestMessage("msg")), getClass().getClassLoader());
  }

  @Test
  public void testDeserializationPermitsPrimitives() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedPackage(TestMessage.class.getName())
            .build();
    TestMessage msg = new TestMessage("msg");
    TestMessage msg2 = serializer.deserialize(serializer.serialize(msg), getClass().getClassLoader());
    assertMsgEquals(msg, msg2);
  }

  @Test
  public void testDeserializationPermitsConcreteInnerClasses() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessage.class)
            .build();
    MyComplexMessage msg = new MyComplexMessage(new MyInnerClass(10,"val"));
    MyComplexMessage msg2 = serializer.deserialize(serializer.serialize(msg), getClass().getClassLoader());
    assertEquals(msg, msg2);
  }

  @Test(expected = IOException.class)
  public void testDeserializationChecksAbstractInnerClasses() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithAbstractType.class)
            .build();
    MyComplexMessageWithAbstractType msg = new MyComplexMessageWithAbstractType(new MyInnerClass(10,"val"));
    serializer.deserialize(serializer.serialize(msg), getClass().getClassLoader());
  }

  @Test
  public void testDeserializationPermitsWhitelistedInnerClasses() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithAbstractType.class)
            .addAllowedClass(MyInnerClass.class)
            .build();
    MyComplexMessageWithAbstractType msg = new MyComplexMessageWithAbstractType(new MyInnerClass(10,"val"));
    MyComplexMessageWithAbstractType msg2 = serializer.deserialize(serializer.serialize(msg), getClass().getClassLoader());
    assertEquals(msg, msg2);
  }

  @Test
  public void testDeserializationWithTypeAlias() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithObjectType.class)
            .addAllowedClass(MyInnerClass.class)
            .addTypeAlias("inner", MyInnerClass.class)
            .build();
    XStreamMessageSerializer deserializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithObjectType.class)
            .addAllowedClass(MyOtherClass.class)
            .addTypeAlias("inner", MyOtherClass.class)
            .build();
    MyComplexMessageWithObjectType msg = new MyComplexMessageWithObjectType(new MyInnerClass(10,"val"));
    MyComplexMessageWithObjectType msg2 = deserializer.deserialize(serializer.serialize(msg), getClass().getClassLoader());
    assertTrue(msg2.innerClass instanceof MyOtherClass);
    assertEquals("val", ((MyOtherClass)msg2.innerClass).value);
  }

  @Test
  public void testDecodingAliasNotEncoding() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithObjectType.class)
            .addAllowedClass(MyInnerClass.class)
            .addDecodingTypeAlias("inner", MyInnerClass.class)
            .build();
    XStreamMessageSerializer deserializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithObjectType.class)
            .addAllowedClass(MyInnerClass.class)
            .build();
    MyComplexMessageWithObjectType msg = new MyComplexMessageWithObjectType(new MyInnerClass(10,"val"));
    MyComplexMessageWithObjectType msg2 = deserializer.deserialize(serializer.serialize(msg), getClass().getClassLoader());
    assertTrue(msg2.innerClass instanceof MyInnerClass);
    assertEquals("val", ((MyInnerClass)msg2.innerClass).value);
  }

  @Test
  public void testDeserializationWithPackageAlias() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithObjectType.class)
            .addAllowedClass(MyInnerClass.class)
            .addPackageAlias("msg", MyInnerClass.class.getPackage().getName())
            .build();
    XStreamMessageSerializer deserializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithObjectType.class)
            .addAllowedClass(no.mnemonic.messaging.requestsink.jms.serializer.packageb.MyInnerClass.class)
            .addPackageAlias("msg", no.mnemonic.messaging.requestsink.jms.serializer.packageb.MyInnerClass.class.getPackage().getName())
            .build();
    MyComplexMessageWithObjectType msg = new MyComplexMessageWithObjectType(new MyInnerClass(10,"val"));
    MyComplexMessageWithObjectType msg2 = deserializer.deserialize(serializer.serialize(msg), getClass().getClassLoader());
    assertTrue(msg2.innerClass instanceof no.mnemonic.messaging.requestsink.jms.serializer.packageb.MyInnerClass);
    assertEquals("val", ((no.mnemonic.messaging.requestsink.jms.serializer.packageb.MyInnerClass)msg2.innerClass).value);
  }

  @Test
  public void testDeserializationWithElementMovedToSubclass() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithObjectType.class)
            .addAllowedClass(MyInnerClass.class)
            .addTypeAlias("inner", MyInnerClass.class)
            .build();
    XStreamMessageSerializer deserializer = XStreamMessageSerializer.builder()
            .addAllowedClass(MyComplexMessageWithObjectType.class)
            .addAllowedClass(MyRefactoredClass.class)
            .addTypeAlias("inner", MyRefactoredClass.class)
            .build();
    MyComplexMessageWithObjectType msg = new MyComplexMessageWithObjectType(new MyInnerClass(10, "val"));
    MyComplexMessageWithObjectType msg2 = deserializer.deserialize(serializer.serialize(msg), getClass().getClassLoader());
    assertTrue(msg2.innerClass instanceof MyRefactoredClass);
    assertEquals("val", ((MyRefactoredClass)msg2.innerClass).value);
    assertEquals(10, ((MyRefactoredClass)msg2.innerClass).abstractValue);
  }

  //helper methods

  private void assertMsgEquals(TestMessage msg, TestMessage msg2) {
    assertEquals(msg.getId(), msg2.getId());
    assertEquals(msg.getCallID(), msg2.getCallID());
    assertEquals(msg.getMessageTimestamp(), msg2.getMessageTimestamp());
  }

  //inner types

  private static class MyComplexMessageWithAbstractType implements Message {
    private MyAbstractClass innerClass;
    public MyComplexMessageWithAbstractType(MyAbstractClass innerClass) {
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
      MyComplexMessageWithAbstractType that = (MyComplexMessageWithAbstractType) o;
      return Objects.equals(innerClass, that.innerClass);
    }

    @Override
    public int hashCode() {
      return Objects.hash(innerClass);
    }
  }

  private static class MyComplexMessageWithObjectType implements Message {
    private Object innerClass;
    public MyComplexMessageWithObjectType(Object innerClass) {
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
      MyComplexMessageWithObjectType that = (MyComplexMessageWithObjectType) o;
      return Objects.equals(innerClass, that.innerClass);
    }

    @Override
    public int hashCode() {
      return Objects.hash(innerClass);
    }
  }


}
