package no.mnemonic.messaging.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.messaging.api.MessagingException;

import javax.jms.*;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * User: joakim
 * Date: 26.apr.2005
 * Time: 10:34:00
 * Id:   $Id$
 */
class JMSUtils {

  private static final Logger LOGGER = Logging.getLogger(JMSUtils.class);

  /**
   * Creates a JMS object (message) from a serializable object
   *
   * @param obj object to create message from
   * @return a JMS message created from given object
   */
  @Deprecated
  static ObjectMessage createObjectMessage(Session session, Serializable obj) throws JMSException {
    ObjectMessage m = session.createObjectMessage(obj);
    m.setStringProperty(JMSBase.PROTOCOL_VERSION_KEY, JMSBase.PROTOCOL_VERSION_13);
    return m;
  }


  /**
   * Creates a JMS object from a string serializable object
   *
   * @param str string to create message from
   * @return a JMS message created from given object
   */
  static TextMessage createTextMessage(Session session, String str, ProtocolVersion protocolVersion) throws JMSException {
    if (protocolVersion == null) throw new IllegalArgumentException("Protocolversion not provided!");
    TextMessage m = session.createTextMessage(str);
    m.setStringProperty(JMSBase.PROTOCOL_VERSION_KEY, protocolVersion.getVersionString());
    return m;
  }

  /**
   * Creates a JMS object from a serializable object.
   * This kind of message is NOT compatible with a V13 JMS consumer class
   *
   * @param data data to create message from
   * @return a JMS message created from given object
   */
  static BytesMessage createByteMessage(Session session, byte[] data) throws JMSException, IOException {
    BytesMessage m = session.createBytesMessage();
    m.writeBytes(data);
    m.setStringProperty(JMSBase.PROTOCOL_VERSION_KEY, JMSBase.PROTOCOL_VERSION_16);
    return m;
  }

  static boolean isCompatible(Message message) throws JMSException {
    String argusCompat = message.getStringProperty(JMSBase.PROTOCOL_VERSION_KEY);
    return SetUtils.in(argusCompat, JMSBase.PROTOCOL_VERSION_13, JMSBase.PROTOCOL_VERSION_16);
  }

  static ProtocolVersion getProtocolVersion(Message message) throws JMSException {
    String argusCompat = message.getStringProperty(JMSBase.PROTOCOL_VERSION_KEY);
    if (SetUtils.in(argusCompat, JMSBase.PROTOCOL_VERSION_16)) {
      return ProtocolVersion.V16;
    }
    return ProtocolVersion.V13;
  }

  static boolean isV16Protocol(Message message) throws JMSException {
    String argusCompat = message.getStringProperty(JMSBase.PROTOCOL_VERSION_KEY);
    return SetUtils.in(argusCompat, JMSBase.PROTOCOL_VERSION_16);
  }

  static void removeMessageListenerAndClose(MessageConsumer consumer) {
    try {
      if (consumer != null) {
        consumer.setMessageListener(null);
        consumer.close();
      }
    } catch (JMSException e) {
      LOGGER.warning(e, "Could not close consumer");
    }
  }

  static void closeProducer(MessageProducer producer) {
    try {
      if (producer != null) {
        producer.close();
      }
    } catch (JMSException e) {
      LOGGER.warning(e, "Could not close consumer");
    }
  }

  static void deleteTemporaryQueue(TemporaryQueue queue) {
    try {
      if (queue != null) {
        queue.delete();
      }
    } catch (JMSException e) {
      LOGGER.warning(e, "Could not delete temporary queue");
    }
  }

  /**
   * Extracts a Serializable from a Message.
   * <p>
   * Extracts a Serializable from an ObjectMessage, a String from a TextMessage, or a Map from a MapMessage. Other
   * message types are not allowed.
   *
   * @param msg JMS message to extract from
   * @return the serializable content of an object message
   * @throws JMSException if a JMS error occurs while extracting message
   */
  @SuppressWarnings("unchecked")
  static <T extends Serializable> T extractObject(Message msg) throws JMSException {
    //noinspection ChainOfInstanceofChecks
    if (msg instanceof ObjectMessage) {
      return (T) ((ObjectMessage) msg).getObject();
    } else if (msg instanceof TextMessage) {
      return (T) ((TextMessage) msg).getText();
    } else if (msg instanceof BytesMessage) {
      return (T) extractSerializableFromBytesMessage((BytesMessage) msg);
    } else if (msg instanceof MapMessage) {
      return (T) extractMapMessage((MapMessage) msg);
    } else {
      throw new MessagingException("message is not of an allowable type: " + msg.getClass().getName());
    }
  }

  private static Serializable extractSerializableFromBytesMessage(BytesMessage msg) throws JMSException {
    try {
      byte[] data = new byte[(int) msg.getBodyLength()];
      msg.readBytes(data);
      //TODO: control the deserialization to ensure only safe classes are deserialized
      return unserialize(data, Thread.currentThread().getContextClassLoader());
    } catch (IOException | ClassNotFoundException e) {
      throw new JMSException(e.getMessage());
    }
  }

  private static Serializable extractMapMessage(MapMessage msg) throws JMSException {
    Enumeration e = msg.getMapNames();
    HashMap<String, Object> map = new HashMap<>();
    while (e.hasMoreElements()) {
      String key = (String) e.nextElement();
      map.put(key, msg.getObject(key));
    }
    return map;
  }

  static void removeExceptionListener(JMSConnection connection, ExceptionListener lst) {
    try {
      connection.removeExceptionListener(lst);
    } catch (Exception e) {
      LOGGER.warning(e, "Could not deregister exception listener");
    }
  }

  static byte[] serialize(Serializable object) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(object);
    oos.close();
    return baos.toByteArray();
  }

  static <T extends Serializable> T unserialize(byte[] data) throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
    //noinspection unchecked
    return (T) ois.readObject();
  }

  static <T extends Serializable> T unserialize(byte[] data, ClassLoader cl) throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ClassLoaderAwareObjectInputStream(new ByteArrayInputStream(data), cl);
    //noinspection unchecked
    return (T) ois.readObject();
  }

  static String hex(byte[] data) {
    StringBuilder sb = new StringBuilder(data.length * 2);
    for (byte b : data)
      sb.append(String.format("%02x", b & 0xff));
    return sb.toString();
  }

  static String md5(byte[] data) {
    try {
      return hex(MessageDigest.getInstance("MD5").digest(data));
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  static MessageDigest md5() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  static byte[] arraySubSeq(byte[] a, int off, int len) {
    if (a == null || a.length == 0) throw new RuntimeException("a can't be empty or null");
    return Arrays.copyOfRange(a, off, off + len);
  }

  /**
   * @param bytes bytes to splitArray
   * @return list of byte arrays, each at most <code>maxlen</code> bytes, or null if bytes are null.
   */
  static List<byte[]> splitArray(byte[] bytes, int maxlen) {
    if (bytes == null) return null;
    List<byte[]> result = new ArrayList<>();
    int off = 0;
    while (off + maxlen < bytes.length) {
      result.add(arraySubSeq(bytes, off, maxlen));
      off += maxlen;
    }
    result.add(arraySubSeq(bytes, off, bytes.length - off));
    return result;
  }

  //TODO: Move to commons
  static class ClassLoaderContext implements AutoCloseable {

    private final ClassLoader contextClassLoader;
    private final ClassLoader originalClassloader;

    public ClassLoaderContext(ClassLoader requestedClassloader) {
      this.contextClassLoader = requestedClassloader;
      this.originalClassloader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(requestedClassloader);
    }

    @Override
    public void close() {
      Thread.currentThread().setContextClassLoader(originalClassloader);
    }

    public static ClassLoaderContext of(ClassLoader cl) {
      return new ClassLoaderContext(cl);
    }

    public static ClassLoaderContext of(Object obj) {
      return new ClassLoaderContext(obj.getClass().getClassLoader());
    }

    public ClassLoader getContextClassLoader() {
      return contextClassLoader;
    }
  }
}
