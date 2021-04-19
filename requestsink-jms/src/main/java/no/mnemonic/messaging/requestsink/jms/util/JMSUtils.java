package no.mnemonic.messaging.requestsink.jms.util;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.MessagingException;
import no.mnemonic.messaging.requestsink.jms.AbstractJMSRequestBase;
import no.mnemonic.messaging.requestsink.jms.ProtocolVersion;
import no.mnemonic.messaging.requestsink.jms.serializer.MessageSerializer;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.naming.NamingException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNull;
import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

/**
 * Package-local utilities, only to be used within this package
 */
public class JMSUtils {

  private static final Logger LOGGER = Logging.getLogger(JMSUtils.class);

  private JMSUtils() {}

  /**
   * Creates a JMS object from a string serializable object
   *
   * @param str string to create message from
   * @return a JMS message created from given object
   */
  public static TextMessage createTextMessage(Session session, String str, ProtocolVersion protocolVersion) throws JMSException {
    assertNotNull(session, "Session not set");
    assertNotNull(str, "String not set");
    assertNotNull(protocolVersion, "ProtocolVersion not set");
    TextMessage m = session.createTextMessage(str);
    m.setStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY, protocolVersion.getVersionString());
    return m;
  }

  /**
   * Creates a JMS object from a serializable object.
   * This kind of message is NOT compatible with a V13 JMS consumer class
   *
   * @param data data to create message from
   * @return a JMS message created from given object
   */
  public static BytesMessage createByteMessage(Session session, byte[] data, ProtocolVersion protocolVersion, String serializerKey) throws JMSException {
    assertNotNull(session, "Session not set");
    assertNotNull(data, "Data not set");
    assertNotNull(protocolVersion, "ProtocolVersion not set");
    BytesMessage m = session.createBytesMessage();
    m.writeBytes(data);
    m.setStringProperty(AbstractJMSRequestBase.SERIALIZER_KEY, serializerKey);
    m.setStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY, protocolVersion.getVersionString());
    return m;
  }

  public static byte[] reassembleFragments(Collection<MessageFragment> fragments, int expectedFragments, String md5Checksum) throws IOException, JMSException, ReassemblyMissingFragmentException {
    if (fragments == null) throw new IllegalArgumentException("message was null");
    if (md5Checksum == null) throw new IllegalArgumentException("md5Checksum was null");

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      if (fragments.size() < expectedFragments) {
        throw new ReassemblyMissingFragmentException(String.format("Expected %d fragments, received %d", expectedFragments, fragments.size()));
      } else if (fragments.size() > expectedFragments) {
        throw new JMSException(String.format("Expected %d fragments, received %d", expectedFragments, fragments.size()));
      }

      //sort fragments in case they are out-of-order
      List<MessageFragment> fragmentList = new ArrayList<>(fragments);
      fragmentList.sort(Comparator.comparing(MessageFragment::getIdx));

      int fragmentIndex = 0;
      MessageDigest digest = md5();

      for (MessageFragment m : fragmentList) {
        //verify that fragment makes sense
        if (fragmentIndex != m.getIdx()) {
          throw new ReassemblyMissingFragmentException(String.format("Got fragment with index %d, expected index %d", m.getIdx(), fragmentIndex));
        }
        //extract data and write to BAOS
        baos.write(m.getData());
        digest.update(m.getData());
        //increment expected fragment index
        fragmentIndex++;
      }
      baos.flush();

      //verify checksum
      String computedChecksum = hex(digest.digest());
      if (!Objects.equals(computedChecksum, md5Checksum)) {
        throw new JMSException("Data checksum mismatch");
      }
      return baos.toByteArray();
    }

  }

  public static void fragment(InputStream messageData, int fragmentSize, FragmentConsumer consumer) throws JMSException {
    if (messageData == null) throw new IllegalArgumentException("messageData was null");
    if (consumer == null) throw new IllegalArgumentException("consumer was null");
    try {
      //create buffer for fragments
      byte[] bytes = new byte[fragmentSize];
      int size;
      int fragmentIndex = 0;
      //create a MD5 digester to calculate a checksum
      MessageDigest digester = md5();
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

  public static boolean isCompatible(Message message) throws JMSException {
    if (!message.propertyExists(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY)) return false;
    String proto = message.getStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY);
    try {
      ProtocolVersion.versionOf(proto);
      return true;
    } catch (JMSException e) {
      return false;
    }
  }

  public static ProtocolVersion getProtocolVersion(Message message) throws JMSException {
    return ProtocolVersion.versionOf(message.getStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY));
  }

  public static void removeMessageListenerAndClose(MessageConsumer consumer) {
    ifNotNullDo(consumer, p -> tryTo(
            () -> {
              p.setMessageListener(null);
              p.close();
            },
            e -> LOGGER.warning(e, "Could not close consumer"))
    );
  }

  public static void closeConsumer(MessageConsumer consumer) {
    ifNotNullDo(consumer,
            c -> tryTo(c::close, e -> LOGGER.warning(e, "Could not close consumer"))
    );
  }

  public static void deleteTemporaryQueue(TemporaryQueue queue) {
    ifNotNullDo(queue,
            q -> tryTo(q::delete, e -> LOGGER.warning(e, "Could not delete temporary queue"))
    );
  }

  public static MessageSerializer determineSerializer(
          Message msg,
          MessageSerializer legacySerializer,
          Map<String, MessageSerializer> serializers
  ) throws JMSException {
    if (msg == null) throw new IllegalArgumentException("msg cannot be null!");
    if (legacySerializer == null) throw new IllegalArgumentException("legacySerializer cannot be null");
    if (serializers == null) serializers = new HashMap<>();

    // Message doesn't specify a serializer, fall back to Java serialization by default.
    if (!msg.propertyExists(AbstractJMSRequestBase.SERIALIZER_KEY)) {
      return legacySerializer;
    }

    // Only V3 and later support custom serializers. For older protocol versions the default is Java serialization.
    if (!getProtocolVersion(msg).atLeast(ProtocolVersion.V3)) {
      return legacySerializer;
    }

    // Pick out serializer specified in message.
    String serializerKey = msg.getStringProperty(AbstractJMSRequestBase.SERIALIZER_KEY);
    if (serializers.containsKey(serializerKey)) {
      return serializers.get(serializerKey);
    }

    // Throw an exception if specified serializer isn't configured server-side
    throw new JMSException("Received message encoded with unknown serializer: " + serializerKey);
  }

  public static byte[] extractMessageBytes(Message msg) throws JMSException {
    if (msg instanceof TextMessage) {
      return ifNotNull(((TextMessage) msg).getText(), String::getBytes);
    } else if (msg instanceof BytesMessage) {
      BytesMessage bmsg = (BytesMessage) msg;
      byte[] data = new byte[(int) bmsg.getBodyLength()];
      bmsg.readBytes(data);
      return data;
    } else {
      throw new MessagingException("message is not of an allowable type: " + msg.getClass().getName());
    }
  }

  public static String hex(byte[] data) {
    if (data == null) return null;
    StringBuilder sb = new StringBuilder(data.length * 2);
    for (byte b : data) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }

  public static String md5(byte[] data) {
    assertNotNull(data, "Data not set");
    return hex(md5().digest(data));
  }

  public static MessageDigest md5() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Error creating MD5 digest", e);
    }
  }

  public static byte[] arraySubSeq(byte[] a, int off, int len) {
    if (a == null || a.length == 0) throw new IllegalArgumentException("a can't be empty or null");
    return Arrays.copyOfRange(a, off, off + len);
  }

  public static <T> T assertNotNull(T obj, String msg) {
    if (obj != null) return obj;
    throw new IllegalArgumentException(msg);
  }

  /**
   * @param bytes bytes to splitArray
   * @return list of byte arrays, each at most <code>maxlen</code> bytes, or null if bytes are null.
   */
  public static List<byte[]> splitArray(byte[] bytes, int maxlen) {
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


  public interface JMSSupplier<T> {
    T get() throws JMSException, NamingException;
  }

  public interface JMSConsumer<T> {
    void apply(T val) throws JMSException, NamingException;
  }
}
