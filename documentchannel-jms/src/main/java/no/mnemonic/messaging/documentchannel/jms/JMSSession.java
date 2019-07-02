package no.mnemonic.messaging.documentchannel.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.IllegalStateException;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicReference;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

public class JMSSession {


  private static final String JAVA_NAMING_FACTORY_INITIAL = "java.naming.factory.initial";
  private static final String JAVA_NAMING_PROVIDER_URL = "java.naming.provider.url";
  private static final String ACTIVEMQ_CONTEXT_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
  private static final String ACTIVEMQ_CONNECTION_FACTORY = "ConnectionFactory";

  private static final Logger LOGGER = Logging.getLogger(JMSSession.class);

  private final InitialContext initialContext;
  private final Connection connection;
  private final Session session;
  private final Destination destination;
  private final AtomicReference<MessageProducer> producer = new AtomicReference<>();
  private final AtomicReference<MessageConsumer> consumer = new AtomicReference<>();

  public enum AcknowledgeMode {
    auto(Session.AUTO_ACKNOWLEDGE),
    client(Session.CLIENT_ACKNOWLEDGE),
    dupsOK(Session.DUPS_OK_ACKNOWLEDGE);

    int mode;

    AcknowledgeMode(int mode) {
      this.mode = mode;
    }
  }

  private JMSSession(
          String contextURL,
          String username,
          String password,
          String destination,
          AcknowledgeMode acknowledgeMode) throws JMSException, NamingException {
    this.initialContext = createInitialContext(contextURL);
    this.destination = lookupDestination(destination);
    this.connection = createConnection(username, password);
    this.session = connection.createSession(false, acknowledgeMode.mode);
  }

  void close() {
    tryTo(session::close, e -> LOGGER.warning(e, "Error closing session"));
    tryTo(connection::close, e -> LOGGER.warning(e, "Error closing connection"));
    ifNotNullDo(producer.get(), p -> tryTo(p::close));
    ifNotNullDo(consumer.get(), c -> tryTo(c::close));
  }

  void commit() throws JMSDocumentChannelException {
    try {
      session.commit();
    } catch (JMSException e) {
      throw new JMSDocumentChannelException(e);
    }
  }

  MessageProducer getProducer() throws JMSDocumentChannelException {
    try {
      return producer.updateAndGet(p -> ifNull(p, this::createProducer));
    } catch (Exception e) {
      throw new JMSDocumentChannelException(e);
    }
  }

  MessageConsumer getConsumer() throws JMSDocumentChannelException {
    try {
      return consumer.updateAndGet(p -> ifNull(p, this::createConsumer));
    } catch (Exception e) {
      throw new JMSDocumentChannelException(e);
    }
  }

  void setExceptionListener(ExceptionListener listener) throws JMSDocumentChannelException {
    try {
      connection.setExceptionListener(listener);
    } catch (JMSException e) {
      throw new JMSDocumentChannelException(e);
    }
  }

  TextMessage createTextMessage(String str) throws JMSDocumentChannelException {
    try {
      TextMessage msg = session.createTextMessage();
      msg.setText(str);
      return msg;
    } catch (JMSException e) {
      throw new JMSDocumentChannelException("Error creating text message", e);
    }
  }

  BytesMessage createByteMessage(byte[] data) throws JMSDocumentChannelException {
    try {
      BytesMessage msg = session.createBytesMessage();
      msg.writeBytes(data);
      return msg;
    } catch (JMSException e) {
      throw new JMSDocumentChannelException("Error creating text message", e);
    }
  }

  void start() throws JMSDocumentChannelException {
    try {
      connection.start();
    } catch (JMSException e) {
      throw new JMSDocumentChannelException("Error starting connection", e);
    }
  }

  //private methods

  private MessageProducer createProducer() {
    try {
      MessageProducer p = session.createProducer(destination);
      p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      return p;
    } catch (JMSException e) {
      throw new IllegalStateException("Error creating producer", e);
    }
  }

  private MessageConsumer createConsumer() {
    try {
      return session.createConsumer(destination);
    } catch (JMSException e) {
      throw new IllegalStateException("Error creating producer", e);
    }
  }

  private InitialContext createInitialContext(String contextURL) throws NamingException {
    Hashtable<String, String> env = new Hashtable<>();
    env.put(JAVA_NAMING_FACTORY_INITIAL, ACTIVEMQ_CONTEXT_FACTORY);
    env.put(JAVA_NAMING_PROVIDER_URL, contextURL);
    return new InitialContext(env);
  }

  private Destination lookupDestination(String destination) throws NamingException {
    return (Destination) initialContext.lookup(destination);
  }

  private Connection createConnection(String username, String password) throws NamingException, JMSException {
    return createConnectionFactory().createConnection(username, password);
  }

  private ConnectionFactory createConnectionFactory() throws NamingException {
    return (ConnectionFactory) initialContext.lookup(ACTIVEMQ_CONNECTION_FACTORY);
  }

  //builder

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    //fields

    private String contextURL;
    private String username;
    private String password;
    private String destination;
    private AcknowledgeMode acknowledgeMode = AcknowledgeMode.auto;

    public JMSSession build() throws NamingException, JMSException {
      return new JMSSession(contextURL, username, password, destination, acknowledgeMode);
    }

    //setters

    public Builder setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
      this.acknowledgeMode = acknowledgeMode;
      return this;
    }

    public Builder setContextURL(String contextURL) {
      this.contextURL = contextURL;
      return this;
    }

    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setDestination(String destination) {
      this.destination = destination;
      return this;
    }
  }

}
