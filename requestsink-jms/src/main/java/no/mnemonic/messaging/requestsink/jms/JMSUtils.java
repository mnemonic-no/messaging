package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.MessagingException;

import javax.jms.*;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

/**
 * Package-local utilities, only to be used within this package
 */
class JMSUtils {

  private static final Logger LOGGER = Logging.getLogger(JMSUtils.class);

  /**
   * Creates a JMS object from a string serializable object
   *
   * @param str string to create message from
   * @return a JMS message created from given object
   */
  static TextMessage createTextMessage(Session session, String str, ProtocolVersion protocolVersion) throws JMSException {
    assertNotNull(session, "Session not set");
    assertNotNull(str, "String not set");
    assertNotNull(protocolVersion, "ProtocolVersion not set");
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
  static BytesMessage createByteMessage(Session session, byte[] data, ProtocolVersion protocolVersion) throws JMSException, IOException {
    assertNotNull(session, "Session not set");
    assertNotNull(data, "Data not set");
    assertNotNull(protocolVersion, "ProtocolVersion not set");
    BytesMessage m = session.createBytesMessage();
    m.writeBytes(data);
    m.setStringProperty(JMSBase.PROTOCOL_VERSION_KEY, protocolVersion.getVersionString());
    return m;
  }

  static byte[] reassembleFragments(Collection<MessageFragment> fragments, int expectedFragments, String md5Checksum) throws IOException, JMSException {
    if (fragments == null) throw new IllegalArgumentException("message was null");
    if (md5Checksum == null) throw new IllegalArgumentException("md5Checksum was null");

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      if (fragments.size() != expectedFragments) {
        throw new JMSException(String.format("Expected %d fragments, received %d", expectedFragments, fragments.size()));
      }

      //sort fragments in case they are out-of-order
      List<MessageFragment> fragmentList = new ArrayList<>(fragments);
      fragmentList.sort(Comparator.comparing(MessageFragment::getIdx));

      int fragmentIndex = 0;
      MessageDigest digest = JMSUtils.md5();

      for (MessageFragment m : fragmentList) {
        //verify that fragment makes sense
        if (fragmentIndex != m.getIdx()) {
          throw new JMSException(String.format("Got fragment with index %d, expected index %d", m.getIdx(), fragmentIndex));
        }
        //extract data and write to BAOS
        baos.write(m.getData());
        digest.update(m.getData());
        //increment expected fragment index
        fragmentIndex++;
      }
      baos.flush();

      //verify checksum
      String computedChecksum = JMSUtils.hex(digest.digest());
      if (!Objects.equals(computedChecksum, md5Checksum)) {
        throw new JMSException("Data checksum mismatch");
      }
      return baos.toByteArray();
    }

  }

  static void fragment(InputStream messageData, int fragmentSize, FragmentConsumer consumer) throws JMSException {
    if (messageData == null) throw new IllegalArgumentException("messageData was null");
    if (consumer == null) throw new IllegalArgumentException("consumer was null");
    try {
      //create buffer for fragments
      byte[] bytes = new byte[fragmentSize];
      int size;
      int fragmentIndex = 0;
      //create a MD5 digester to calculate a checksum
      MessageDigest digester = JMSUtils.md5();
      //read each fragment
      while ((size = messageData.read(bytes)) >= 0) {
        digester.update(bytes, 0, size);
        consumer.fragment(arraySubSeq(bytes, 0, size), fragmentIndex++);
      }
      consumer.end(fragmentIndex, digester.digest());
    } catch (IOException e) {
      throw new JMSException("Error reading data", e.getMessage());
    }
  }

  interface FragmentConsumer {
    void fragment(byte[] data, int idx) throws JMSException, IOException;

    void end(int fragments, byte[] digest) throws JMSException;
  }

  static boolean isCompatible(Message message) throws JMSException {
    if (!message.propertyExists(JMSBase.PROTOCOL_VERSION_KEY)) return false;
    String proto = message.getStringProperty(JMSBase.PROTOCOL_VERSION_KEY);
    try {
      ProtocolVersion.versionOf(proto);
      return true;
    } catch (JMSException e) {
      return false;
    }
  }

  static ProtocolVersion getProtocolVersion(Message message) throws JMSException {
    return ProtocolVersion.versionOf(message.getStringProperty(JMSBase.PROTOCOL_VERSION_KEY));
  }

  static void removeMessageListenerAndClose(MessageConsumer consumer) {
    ifNotNullDo(consumer, p -> tryTo(
            () -> {
              p.setMessageListener(null);
              p.close();
            },
            e -> LOGGER.warning(e, "Could not close consumer"))
    );
  }

  static void closeProducer(MessageProducer producer) {
    ifNotNullDo(producer,
            p -> tryTo(p::close, e -> LOGGER.warning(e, "Could not close producer"))
    );
  }

  static void deleteTemporaryQueue(TemporaryQueue queue) {
    ifNotNullDo(queue,
            q -> tryTo(q::delete, e -> LOGGER.warning(e, "Could not delete temporary queue"))
    );
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
    if (msg instanceof TextMessage) {
      return (T) ((TextMessage) msg).getText();
    } else if (msg instanceof BytesMessage) {
      return (T) extractSerializableFromBytesMessage((BytesMessage) msg);
    } else {
      throw new MessagingException("message is not of an allowable type: " + msg.getClass().getName());
    }
  }

  static void removeExceptionListener(JMSConnection connection, ExceptionListener listener) {
    assertNotNull(connection, "Connection not set");
    assertNotNull(listener, "Listener not set");
    tryTo(() -> connection.removeExceptionListener(listener), e -> LOGGER.warning(e, "Could not deregister exception listener"));
  }

  static byte[] serialize(Serializable object) throws IOException {
    assertNotNull(object, "Object not set");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(object);
    oos.close();
    return baos.toByteArray();
  }

  static <T extends Serializable> T unserialize(byte[] data) throws IOException, ClassNotFoundException {
    assertNotNull(data, "Data not set");
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
    //noinspection unchecked
    return (T) ois.readObject();
  }

  static <T extends Serializable> T unserialize(byte[] data, ClassLoader classLoader) throws IOException, ClassNotFoundException {
    assertNotNull(data, "Data not set");
    assertNotNull(classLoader, "ClassLoader not set");
    ObjectInputStream ois = new ClassLoaderAwareObjectInputStream(new ByteArrayInputStream(data), classLoader);
    //noinspection unchecked
    return (T) ois.readObject();
  }

  static String hex(byte[] data) {
    if (data == null) return null;
    StringBuilder sb = new StringBuilder(data.length * 2);
    for (byte b : data) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }

  static String md5(byte[] data) {
    assertNotNull(data, "Data not set");
    return hex(md5().digest(data));
  }

  static MessageDigest md5() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  static byte[] arraySubSeq(byte[] a, int off, int len) {
    if (a == null || a.length == 0) throw new IllegalArgumentException("a can't be empty or null");
    return Arrays.copyOfRange(a, off, off + len);
  }

  static <T> T assertNotNull(T obj, String msg) {
    if (obj != null) return obj;
    throw new IllegalArgumentException(msg);
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

  //private methods

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

}
