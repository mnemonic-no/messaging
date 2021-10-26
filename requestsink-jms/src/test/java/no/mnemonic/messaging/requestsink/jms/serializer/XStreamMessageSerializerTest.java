package no.mnemonic.messaging.requestsink.jms.serializer;

import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.jms.IllegalDeserializationException;
import no.mnemonic.messaging.requestsink.jms.TestMessage;
import no.mnemonic.messaging.requestsink.jms.serializer.packagea.MyAbstractClass;
import no.mnemonic.messaging.requestsink.jms.serializer.packagea.MyComplexMessage;
import no.mnemonic.messaging.requestsink.jms.serializer.packagea.MyInnerClass;
import no.mnemonic.messaging.requestsink.jms.serializer.packagea.MyOtherClass;
import no.mnemonic.messaging.requestsink.jms.serializer.packagea.MyRefactoredClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class XStreamMessageSerializerTest {

  @Test
  public void testDeserializerHandlesKnownLiteral() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(TestMessage.class)
            .build();
    String xml = "<no.mnemonic.messaging.requestsink.jms.TestMessage>\n" +
            "  <id>msg</id>\n" +
            "  <enumField>literal1</enumField>\n" +
            "</no.mnemonic.messaging.requestsink.jms.TestMessage>\n";
    TestMessage deserialized = serializer.deserialize(xml.getBytes(), getClass().getClassLoader());
    assertEquals("msg", deserialized.getId());
    assertSame(TestMessage.MyEnum.literal1, deserialized.enumField);
  }

  @Test
  public void testDeserializerFailsOnUnknownEnumLiteral() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(TestMessage.class)
            .build();
    String xml = "<no.mnemonic.messaging.requestsink.jms.TestMessage>\n" +
            "  <id>msg</id>\n" +
            "  <enumField>invalidLiteral</enumField>\n" +
            "</no.mnemonic.messaging.requestsink.jms.TestMessage>\n";
    assertThrows(IOException.class, ()->serializer.deserialize(xml.getBytes(), getClass().getClassLoader()));
  }

  @Test
  public void testDeserializerIgnoresUnknownEnumLiteral() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(TestMessage.class)
            .setIgnoreUnknownEnumLiterals(true)
            .build();
    String xml = "<no.mnemonic.messaging.requestsink.jms.TestMessage>\n" +
            "  <id>msg</id>\n" +
            "  <enumField>invalidLiteral</enumField>\n" +
            "</no.mnemonic.messaging.requestsink.jms.TestMessage>\n";
    TestMessage deserialized = serializer.deserialize(xml.getBytes(), getClass().getClassLoader());
    assertEquals("msg", deserialized.getId());
    assertNull(deserialized.enumField);
  }

  @Test
  public void testDeserializerIgnoresUnknownProperty() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(TestMessage.class)
            .build();
    String xml = "<no.mnemonic.messaging.requestsink.jms.TestMessage>\n" +
            "  <id>msg</id>\n" +
            "  <newProperty>msg</newProperty>\n" +
            "</no.mnemonic.messaging.requestsink.jms.TestMessage>\n";
    TestMessage deserialized = serializer.deserialize(xml.getBytes(), getClass().getClassLoader());
    assertEquals("msg", deserialized.getId());
    assertNull(deserialized.getCallID());
    assertEquals(0L, deserialized.getMessageTimestamp());
  }

  @Test
  public void testDeserializerPermitsFieldReference() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(TestMessage.class)
            .build();
    String xml = "<no.mnemonic.messaging.requestsink.jms.TestMessage>\n" +
            "  <objectField1>\n" +
            "    <field>value</field>\n" +
            "  </objectField1>\n" +
            "  <objectField2 reference=\"../objectField1\"/>\n" +
            "</no.mnemonic.messaging.requestsink.jms.TestMessage>\n";
    TestMessage deserialized = serializer.deserialize(xml.getBytes(), getClass().getClassLoader());
    assertEquals("value", deserialized.objectField1.field);
    assertEquals("value", deserialized.objectField2.field);
  }

  /**
   * Test to check for known issue
   * Followed up towards XStream in ticket https://github.com/x-stream/xstream/issues/269
   */
  @Test
  public void testDeserializerFailsWithInvalidReference() {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(TestMessage.class)
            .build();
    String xml = "<no.mnemonic.messaging.requestsink.jms.TestMessage>\n" +
            "  <objectField2 reference=\"../objectField3\"/>\n" +
            "</no.mnemonic.messaging.requestsink.jms.TestMessage>\n";
    assertThrows(IOException.class, ()->serializer.deserialize(xml.getBytes(), getClass().getClassLoader()));
  }

  @Test(expected = IllegalDeserializationException.class)
  public void testNonAllowedClassNotDeserialized() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass("org.acme.*")
            .build();
    serializer.deserialize(serializer.serialize(new TestMessage("msg")), getClass().getClassLoader());
  }

  @Test
  public void testDeserializationPermitsPrimitives() throws IOException {
    XStreamMessageSerializer serializer = XStreamMessageSerializer.builder()
            .addAllowedClass(TestMessage.class.getName())
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

  @Test(expected = IllegalDeserializationException.class)
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
