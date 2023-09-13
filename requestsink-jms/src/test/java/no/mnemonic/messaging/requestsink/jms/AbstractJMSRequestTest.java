package no.mnemonic.messaging.requestsink.jms;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.nio.CharBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Random;

abstract class AbstractJMSRequestTest {

  private static final char[] COOKIE_CHARACTERS = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789".toCharArray();
  private static Random random = new SecureRandom();

  Connection testConnection;
  Session session;

  TestMessage createBigResponse() {
    char[] msg = new char[1500];
    Arrays.fill(msg, 'c');
    return new TestMessage(new String(msg));
  }

  public interface JMSAction {
    void action(Message msg) throws Exception;
  }

  TextMessage textMsg(String text, String messageType, String callID, JMSRequestProxyTest.JMSAction... actions) throws Exception {
    TextMessage msg = session.createTextMessage(text);
    msg.setStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY, ProtocolVersion.V3.getVersionString());
    msg.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, messageType);
    msg.setJMSCorrelationID(callID);
    if (actions != null) {
      for (JMSRequestProxyTest.JMSAction t : actions) t.action(msg);
    }
    return msg;
  }

  BytesMessage byteMsg(no.mnemonic.messaging.requestsink.Message obj, String messageType, String callID) throws JMSException, IOException {
    BytesMessage msg = session.createBytesMessage();
    msg.writeBytes(TestUtils.serialize(obj));
    msg.setStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY, ProtocolVersion.V3.getVersionString());
    msg.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, messageType);
    msg.setJMSCorrelationID(callID);
    return msg;
  }

  BytesMessage byteMsg(byte[] data, String messageType, String callID) throws JMSException, IOException {
    BytesMessage msg = session.createBytesMessage();
    msg.writeBytes(data);
    msg.setStringProperty(AbstractJMSRequestBase.PROTOCOL_VERSION_KEY, ProtocolVersion.V3.getVersionString());
    msg.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, messageType);
    msg.setJMSCorrelationID(callID);
    return msg;
  }

  Session createSession() throws NamingException, JMSException {
    ConnectionFactory connectionFactory = (ConnectionFactory) createInitialContext().lookup("ConnectionFactory");
    testConnection = connectionFactory.createConnection();
    testConnection.start();
    return testConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  Destination lookupDestination(String name) throws NamingException {
    return (Destination) createInitialContext().lookup(name);
  }

  private InitialContext createInitialContext() throws NamingException {
    Hashtable<String, String> env = new Hashtable<>();
    env.put(InitialContext.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
    env.put(InitialContext.PROVIDER_URL, "vm://localhost?broker.persistent=false&broker.useJmx=false");
    //noinspection unchecked
    env.put("trustAllPackages", "true");
    return new InitialContext(env);
  }

  static String generateCookie(int length) {
    return generateCookie(length, COOKIE_CHARACTERS);
  }

  private static String generateCookie(int length, char[] charset) {
    CharBuffer buf = CharBuffer.allocate(length);
    for (int i = 0; i < length; i++) {
      buf.array()[i] = (charset[(random.nextInt(charset.length))]);
    }
    return new String(buf.array());
  }

  <T extends AbstractJMSRequestBase.BaseBuilder<T>> T addConnection(T builder) {
    //set up a real JMS connection to a vm-local activemq
    return builder
            .setContextFactoryName("org.apache.activemq.jndi.ActiveMQInitialContextFactory")
            .setContextURL("vm://localhost?broker.persistent=false&broker.useJmx=false")
            .setConnectionFactoryName("ConnectionFactory")
            .setConnectionProperty("trustAllPackages", "true");
  }
}
