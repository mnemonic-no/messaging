package no.mnemonic.messaging.jms;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.CharBuffer;
import java.security.SecureRandom;
import java.util.Hashtable;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

import static no.mnemonic.messaging.jms.JMSBase.*;
import static no.mnemonic.messaging.jms.JMSRequestProxy.PROPERTY_MESSAGE_TYPE;

abstract class AbstractJMSRequestTest {

  private static final char[] COOKIE_CHARACTERS = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789".toCharArray();
  private static Random random = new SecureRandom();

  Connection testConnection;
  Session session;

  public interface JMSAction {
    void action(Message msg) throws Exception;
  }


  ObjectMessage objMsg(Serializable obj, String messageType, String callID) throws JMSException {
    ObjectMessage msg = session.createObjectMessage(obj);
    msg.setStringProperty(PROTOCOL_VERSION_KEY, PROTOCOL_VERSION_13);
    msg.setStringProperty(PROPERTY_MESSAGE_TYPE, messageType);
    msg.setJMSCorrelationID(callID);
    return msg;
  }

  TextMessage textMsg(String text, String messageType, String callID, boolean v16, JMSRequestProxyTest.JMSAction... actions) throws Exception {
    TextMessage msg = session.createTextMessage(text);
    msg.setStringProperty(PROTOCOL_VERSION_KEY, v16 ? PROTOCOL_VERSION_16 : PROTOCOL_VERSION_13);
    msg.setStringProperty(PROPERTY_MESSAGE_TYPE, messageType);
    msg.setJMSCorrelationID(callID);
    if (actions != null) {
      for (JMSRequestProxyTest.JMSAction t : actions) t.action(msg);
    }
    return msg;
  }

  BytesMessage byteMsg(Serializable obj, String messageType, String callID) throws JMSException, IOException {
    BytesMessage msg = session.createBytesMessage();
    msg.writeBytes(JMSUtils.serialize(obj));
    msg.setStringProperty(PROTOCOL_VERSION_KEY, PROTOCOL_VERSION_16);
    msg.setStringProperty(PROPERTY_MESSAGE_TYPE, messageType);
    msg.setJMSCorrelationID(callID);
    return msg;
  }

  BytesMessage byteMsg(byte[] data, String messageType, String callID) throws JMSException, IOException {
    BytesMessage msg = session.createBytesMessage();
    msg.writeBytes(data);
    msg.setStringProperty(PROTOCOL_VERSION_KEY, PROTOCOL_VERSION_16);
    msg.setStringProperty(PROPERTY_MESSAGE_TYPE, messageType);
    msg.setJMSCorrelationID(callID);
    return msg;
  }

  Session createSession(boolean transacted) throws NamingException, JMSException {
    ConnectionFactory connectionFactory = (ConnectionFactory) createInitialContext().lookup("ConnectionFactory");
    testConnection = connectionFactory.createConnection();
    testConnection.start();
    return testConnection.createSession(transacted, transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
  }

  Destination createDestination(String name) throws NamingException {
    return (Destination) createInitialContext().lookup(name);
  }

  private InitialContext createInitialContext() throws NamingException {
    Hashtable<String, String> env = new Hashtable<>();
    env.put(InitialContext.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
    env.put(InitialContext.PROVIDER_URL, "vm://localhost?broker.persistent=false");
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

  JMSConnection createConnection() {
    //set up a real JMS connection to a vm-local activemq
    return JMSConnectionImpl.builder()
            .setContextFactoryName("org.apache.activemq.jndi.ActiveMQInitialContextFactory")
            .setContextURL("vm://localhost?broker.persistent=false")
            .setConnectionFactoryName("ConnectionFactory")
            .setProperty("trustAllPackages", "true")
            .build();
  }


}
