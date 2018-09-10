package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.AppendMembers;
import no.mnemonic.commons.utilities.AppendUtils;
import no.mnemonic.commons.utilities.StringUtils;
import no.mnemonic.messaging.requestsink.jms.util.JMSUtils;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.IllegalStateException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"ClassReferencesSubclass"})
public abstract class AbstractJMSRequestBase implements LifecycleAspect, AppendMembers, ExceptionListener {

  private static final Logger LOGGER = Logging.getLogger(AbstractJMSRequestBase.class);

  static final int DEFAULT_MAX_MAX_MESSAGE_SIZE = 100000;
  static final int DEFAULT_PRIORITY = 1;

  public static final String SERIALIZER_KEY = "ArgusMessagingSerializer";
  public static final String PROTOCOL_VERSION_KEY = "ArgusMessagingProtocol";
  public static final String PROPERTY_MESSAGE_TYPE = "MessageType";
  public static final String MESSAGE_TYPE_STREAM_CLOSED = "JMSStreamClosed";
  public static final String MESSAGE_TYPE_END_OF_FRAGMENTED_MESSAGE = "JMSFragmentedMessageEnd";
  public static final String MESSAGE_TYPE_EXCEPTION = "JMSException";
  public static final String MESSAGE_TYPE_SIGNAL = "JMSSignal";
  public static final String MESSAGE_TYPE_CHANNEL_REQUEST = "JMSChannelRequest";
  public static final String MESSAGE_TYPE_CHANNEL_SETUP = "JMSChannelSetup";
  public static final String MESSAGE_TYPE_SIGNAL_FRAGMENT = "JMSSignalFragment";
  public static final String MESSAGE_TYPE_SIGNAL_RESPONSE = "JMSSignalResponse";
  public static final String MESSAGE_TYPE_EXTEND_WAIT = "JMSExtendWait";
  public static final String PROPERTY_REQ_TIMEOUT = "RequestTimeout";
  public static final String PROPERTY_FRAGMENTS_TOTAL = "TotalFragments";
  public static final String PROPERTY_FRAGMENTS_IDX = "FragmentIndex";
  public static final String PROPERTY_RESPONSE_ID = "ResponseID";
  public static final String PROPERTY_DATA_CHECKSUM_MD5 = "DataChecksumMD5";

  static final String ERROR_CLOSED = "closed";

  // common properties

  private final String contextFactoryName;
  private final String contextURL;
  private final String connectionFactoryName;
  private final String username;
  private final String password;
  private final Map<String, String> connectionProperties;
  private final String destinationName;
  private final int priority;
  private final int maxMessageSize;

  // variables

  private final AtomicReference<InitialContext> initialContext = new AtomicReference<>();
  final AtomicBoolean closed = new AtomicBoolean();
  final AtomicReference<Connection> connection = new AtomicReference<>();
  final AtomicReference<Session> session = new AtomicReference<>();
  final AtomicReference<Destination> destination = new AtomicReference<>();

  // ************************* constructors ********************************

  AbstractJMSRequestBase(String contextFactoryName, String contextURL, String connectionFactoryName,
                         String username, String password, Map<String, String> connectionProperties,
                         String destinationName, int priority, int maxMessageSize) {

    if (StringUtils.isBlank(contextFactoryName)) {
      throw new IllegalArgumentException("contextFactoryName not set");
    }
    if (StringUtils.isBlank(contextURL)) {
      throw new IllegalArgumentException("contextURL not set");
    }
    if (StringUtils.isBlank(connectionFactoryName)) {
      throw new IllegalArgumentException("connectionFactoryName not set");
    }
    if (StringUtils.isBlank(destinationName)) {
      throw new IllegalArgumentException("No destination name provided");
    }
    if (maxMessageSize < 1) {
      throw new IllegalArgumentException("maxMessageSize cannot be lower than 1");
    }
    if (priority < 1) {
      throw new IllegalArgumentException("priority cannot be lower than 1");
    }
    this.contextFactoryName = contextFactoryName;
    this.contextURL = contextURL;
    this.connectionFactoryName = connectionFactoryName;
    this.username = username;
    this.password = password;
    this.connectionProperties = connectionProperties;
    this.destinationName = destinationName;
    this.priority = priority;
    this.maxMessageSize = maxMessageSize;
  }

  // ************************ interface methods ***********************************


  @Override
  public void appendMembers(StringBuilder buf) {
    AppendUtils.appendField(buf, "contextURL", contextURL);
    AppendUtils.appendField(buf, "connectionFactoryName", connectionFactoryName);
    AppendUtils.appendField(buf, "destinationName", destinationName);
    AppendUtils.appendField(buf, "priority", priority);
  }

  @Override
  public String toString() {
    return AppendUtils.toString(this);
  }

  // ******************** protected and private methods ***********************

  boolean isClosed() {
    return closed.get();
  }

  int getPriority() {
    return priority;
  }

  int getMaxMessageSize() {
    return maxMessageSize;
  }

  private InitialContext getInitialContext() throws NamingException, JMSException {
    if (isClosed()) throw new IllegalStateException(ERROR_CLOSED);
    return getOrUpdateSynchronized(initialContext, this::createInitialContext);
  }

  private Connection getConnection() throws JMSException, NamingException {
    if (isClosed()) throw new IllegalStateException(ERROR_CLOSED);
    return getOrUpdateSynchronized(connection, this::createConnection);
  }

  Destination getDestination() throws JMSException, NamingException {
    if (isClosed()) throw new IllegalStateException(ERROR_CLOSED);
    return getOrUpdateSynchronized(destination, this::lookupDestination);
  }

  Session getSession() throws JMSException, NamingException {
    if (isClosed()) throw new IllegalStateException(ERROR_CLOSED);
    return getOrUpdateSynchronized(session, this::createSession);
  }

  private Session createSession() throws NamingException, JMSException {
    LOGGER.debug("Creating session");
    return getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  private Destination lookupDestination() throws NamingException, JMSException {
    LOGGER.debug("Looking up destination %s", destinationName);
    return lookupDestination(destinationName);
  }

  private InitialContext createInitialContext() throws NamingException {
    LOGGER.debug("Creating initial context for %s", contextURL);
    Hashtable<String, String> env = new Hashtable<>();
    env.put(InitialContext.INITIAL_CONTEXT_FACTORY, contextFactoryName);
    env.put(InitialContext.PROVIDER_URL, contextURL);
    connectionProperties.forEach(env::put);
    return new InitialContext(env);
  }

  private Destination lookupDestination(String destinationName) throws JMSException, NamingException {
    if (StringUtils.isBlank(destinationName)) throw new NamingException("Destination name not set");
    Object obj = getInitialContext().lookup(destinationName);
    // error if no such destination
    if (obj == null) {
      throw new NamingException(destinationName + ": no such Destination");
    }
    // sanity check
    if (!(obj instanceof Destination)) {
      throw new JMSException(destinationName + ": not a destination (" + obj.getClass().getName() + ")");
    }
    return (Destination) obj;
  }

  private Connection createConnection() throws JMSException, NamingException {
    LOGGER.info("Creating new JMS connection to %s", contextURL);
    // fetch factory from JNDI
    Object obj = getInitialContext().lookup(connectionFactoryName);
    if (obj == null) {
      throw new NamingException(connectionFactoryName + ": no such ConnectionFactory");
    }
    if (!(obj instanceof ConnectionFactory)) {
      throw new JMSException(connectionFactoryName + ": not a ConnectionFactory (" + obj.getClass() + ")");
    }

    // create connection
    ConnectionFactory connectionFactory = (ConnectionFactory) obj;
    Connection conn;
    if (username != null && password != null) {
      conn = connectionFactory.createConnection(username, password);
    } else {
      conn = connectionFactory.createConnection();
    }
    conn.setExceptionListener(this);
    conn.start();

    return conn;
  }

  private <U> U getOrUpdateSynchronized(AtomicReference<U> ref, JMSUtils.JMSSupplier<U> task) throws NamingException, JMSException {
    U current = ref.get();
    if (current != null) return current;
    //synchronize only if null, to avoid contention
    //need to synchronize with possible reconnect
    synchronized (this) {
      try {
        return ref.updateAndGet(t -> {
          if (t != null) return t;
          try {
            return task.get();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        });
      } catch (RuntimeException e) {
        if (e.getCause() instanceof JMSException) throw (JMSException) e.getCause();
        if (e.getCause() instanceof NamingException) throw (NamingException) e.getCause();
        throw e;
      }
    }
  }


  <T> void executeAndReset(AtomicReference<T> ref, JMSUtils.JMSConsumer<T> op, String errorString) {
    T val = ref.getAndUpdate(t -> null);
    if (val != null) {
      try {
        op.apply(val);
      } catch (Exception e) {
        LOGGER.warning(e, errorString);
      }
    }
  }

  // ************************* property accessors ********************************

  public static class BaseBuilder<T extends BaseBuilder> {
    String contextFactoryName;
    String contextURL;
    String connectionFactoryName;
    String username;
    String password;
    final Map<String, String> connectionProperties = new HashMap<>();
    String destinationName;
    int maxMessageSize = DEFAULT_MAX_MAX_MESSAGE_SIZE;
    int priority = DEFAULT_PRIORITY;

    public T setContextFactoryName(String contextFactoryName) {
      this.contextFactoryName = contextFactoryName;
      //noinspection unchecked
      return (T) this;
    }

    public T setContextURL(String contextURL) {
      this.contextURL = contextURL;
      //noinspection unchecked
      return (T) this;
    }

    public T setConnectionFactoryName(String connectionFactoryName) {
      this.connectionFactoryName = connectionFactoryName;
      //noinspection unchecked
      return (T) this;
    }

    public T setUsername(String username) {
      this.username = username;
      //noinspection unchecked
      return (T) this;
    }

    public T setPassword(String password) {
      this.password = password;
      //noinspection unchecked
      return (T) this;
    }

    public T setConnectionProperties(Map<String, String> connectionProperties) {
      this.connectionProperties.putAll(connectionProperties);
      //noinspection unchecked
      return (T) this;
    }

    public T setConnectionProperty(String key, String value) {
      this.connectionProperties.put(key, value);
      //noinspection unchecked
      return (T) this;
    }

    public T setDestinationName(String destinationName) {
      this.destinationName = destinationName;
      //noinspection unchecked
      return (T) this;
    }

    public T setMaxMessageSize(int maxMessageSize) {
      this.maxMessageSize = maxMessageSize;
      //noinspection unchecked
      return (T) this;
    }

    public T setPriority(int priority) {
      this.priority = priority;
      //noinspection unchecked
      return (T) this;
    }
  }

}
