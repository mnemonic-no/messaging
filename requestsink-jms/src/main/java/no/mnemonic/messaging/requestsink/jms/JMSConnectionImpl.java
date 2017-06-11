package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.AppendMembers;
import no.mnemonic.commons.utilities.AppendUtils;
import no.mnemonic.commons.utilities.StringUtils;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.MessagingException;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.IllegalStateException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.collections.MapUtils.map;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

public class JMSConnectionImpl implements JMSConnection, ExceptionListener, LifecycleAspect, AppendMembers {

  private static final Logger LOGGER = Logging.getLogger(JMSConnectionImpl.class);

  // properties

  private final String contextFactoryName;
  private final String contextURL;
  private final String connectionFactoryName;
  private final String clientID;
  private final String username;
  private final String password;
  private final Properties properties;
  private final AtomicReference<ExecutorService> executor = new AtomicReference<>();

  // variables

  private final AtomicBoolean invalidating = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<InitialContext> initialContext = new AtomicReference<>();
  private final AtomicReference<Connection> connection = new AtomicReference<>();
  private final Collection<JMSBase> clients = Collections.synchronizedSet(new HashSet<>());
  private final Set<ExceptionListener> exceptionListeners = Collections.synchronizedSet(new HashSet<>());

  private JMSConnectionImpl(
          String contextFactoryName, String contextURL, String connectionFactoryName,
          String clientID, String username, String password,
          Map<String, String> properties
  ) {
    this.contextFactoryName = contextFactoryName;
    this.contextURL = contextURL;
    this.connectionFactoryName = connectionFactoryName;
    this.clientID = clientID;
    this.username = username;
    this.password = password;
    Properties p = new Properties();
    properties.forEach(p::setProperty);
    this.properties = p;
  }

  // interface methods

  @Override
  public void appendMembers(StringBuilder buf) {
    AppendUtils.appendField(buf, "contextFactoryName", contextFactoryName);
    AppendUtils.appendField(buf, "connectionFactoryName", connectionFactoryName);
    AppendUtils.appendField(buf, "contextURL", contextURL);
    AppendUtils.appendField(buf, "clientID", clientID);
  }

  @Override
  public String toString() {
    return AppendUtils.toString(this);
  }

  @Override
  public void startComponent() {
    executor.set(Executors.newSingleThreadExecutor());
  }

  @Override
  public void stopComponent() {
    closed.set(true);
    invalidateInSeparateThread();
    executor.get().shutdown();
    LambdaUtils.tryTo(() -> executor.get().awaitTermination(10, TimeUnit.SECONDS));
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void onException(JMSException e) {
    if (invalidating.get()) return;
    exceptionListeners.forEach(l -> tryTo(() -> l.onException(e)));
  }

  @Override
  public void addExceptionListener(ExceptionListener listener) {
    exceptionListeners.add(listener);
  }

  @Override
  public void removeExceptionListener(ExceptionListener listener) {
    exceptionListeners.remove(listener);
  }

  @Override
  public Destination lookupDestination(String destinationName) throws JMSException, NamingException {
    if (StringUtils.isBlank(destinationName)) throw new NamingException("Destination name not set");
    try {
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
    } catch (NamingException | JMSException e) {
      invalidateInSeparateThread();
      throw e;
    }
  }

  @Override
  public Session getSession(JMSBase client, boolean transacted) throws JMSException, NamingException {
    try {
      return getConnection(client)
              .createSession(transacted, transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
    } catch (JMSException | NamingException e) {
      invalidateInSeparateThread();
      throw e;
    } catch (Exception e) {
      invalidateInSeparateThread();
      throw new MessagingException(e);
    }
  }

  @Override
  public void register(JMSBase client) {
    if (client == null) return;
    LOGGER.info("Registering JMS client " + client);
    clients.add(client);
  }

  @Override
  public void deregister(JMSBase client) {
    if (client == null) return;
    LOGGER.info("Deregistering JMS client " + client);
    clients.remove(client);
    if (client instanceof ExceptionListener) {
      exceptionListeners.remove(client);
    }
  }

  @Override
  public void invalidate() {
    // avoid multiple calls to this method
    if (!invalidating.compareAndSet(false, true)) {
      return;
    }

    try {
      // invalidate all clients
      LOGGER.info("Invalidating %d clients", clients.size());
      for (JMSBase client : new ArrayList<>(clients)) {
        try {
          client.invalidate();
        } catch (Exception e) {
          //do nothing
        }
      }
    } finally {
      invalidating.set(false);
      // finally reset connection
      clients.clear();
      exceptionListeners.clear();
      // close this connection
      ifNotNullDo(connection.getAndSet(null), c -> tryTo(c::close));
      ifNotNullDo(initialContext.getAndSet(null), c -> tryTo(c::close));
    }
  }

  @Override
  public void close() {
    invalidate();
  }

  //protected and private methods

  /**
   * Fetch connection for this jms object
   *
   * @param client the client which requests a connection
   * @return an active connection
   * @throws JMSException    if creating the connection throws an exception
   * @throws NamingException if the initial context throws exception for JNDI lookup
   */
  private Connection getConnection(JMSBase client) throws JMSException, NamingException {
    if (isClosed()) throw new RuntimeException("closed");
    register(client);
    Connection conn = connection.get();
    if (conn != null) return conn;

    LOGGER.info("Creating new JMS connection to %s", contextURL);
    // fetch factory from JNDI
    Object obj = getInitialContext().lookup(connectionFactoryName);
    if (obj == null) {
      throw new NamingException(connectionFactoryName + ": no such ConnectionFactory");
    }
    if (!(obj instanceof ConnectionFactory)) {
      throw new JMSException(connectionFactoryName + ": not a ConnectionFactory ("
              + obj.getClass().getName() + ")");
    }

    // create connection
    ConnectionFactory connectionFactory = (ConnectionFactory) obj;
    if (username != null && password != null) {
      conn = connectionFactory.createConnection(username, password);
    } else {
      conn = connectionFactory.createConnection();
    }
    conn.setExceptionListener(this);

    // prepare connection
    if (clientID != null) {
      conn.setClientID(clientID);
    }
    conn.start();
    connection.set(conn);

    return conn;
  }

  /**
   * Create initial context for jms lookup
   *
   * @return initial context (cached or new)
   * @throws NamingException if an error occurs creating initial context
   */
  private InitialContext getInitialContext() throws NamingException {
    if (isClosed()) throw new RuntimeException("closed");
    synchronized (this) {
      if (initialContext.get() == null) {
        LOGGER.debug("Creating initial context for %s", contextURL);
        Hashtable<String, String> env = new Hashtable<>();
        env.put(InitialContext.INITIAL_CONTEXT_FACTORY, contextFactoryName);
        env.put(InitialContext.PROVIDER_URL, contextURL);
        properties.forEach((k, v) -> env.put((String) k, (String) v));
        initialContext.set(new InitialContext(env));
      }
      return initialContext.get();
    }
  }

  private void invalidateInSeparateThread() {
    if (executor.get() == null) throw new IllegalStateException("Component not started");
    executor.get().submit(this::invalidate);
  }

  //builder

  @SuppressWarnings("WeakerAccess")
  public static Builder builder() {
    return new Builder();
  }

  @SuppressWarnings({"WeakerAccess", "SameParameterValue", "SpellCheckingInspection", "unused"})
  public static class Builder {

    //fields
    private String contextFactoryName;
    private String contextURL;
    private String connectionFactoryName;
    private String clientID;
    private String username;
    private String password;
    private Map<String, String> properties = new HashMap<>();

    private Builder() {
    }

    public JMSConnectionImpl build() {
      if (StringUtils.isBlank(contextFactoryName)) throw new IllegalArgumentException("contextFactoryName not set");
      if (StringUtils.isBlank(contextURL)) throw new IllegalArgumentException("contextURL not set");
      if (StringUtils.isBlank(connectionFactoryName))
        throw new IllegalArgumentException("connectionFactoryName not set");
      return new JMSConnectionImpl(
              contextFactoryName, contextURL,
              connectionFactoryName, clientID, username, password,
              properties);
    }

    //setters

    public Builder setContextFactoryName(String contextFactoryName) {
      this.contextFactoryName = contextFactoryName;
      return this;
    }

    public Builder setContextURL(String contextURL) {
      this.contextURL = contextURL;
      return this;
    }

    public Builder setConnectionFactoryName(String connectionFactoryName) {
      this.connectionFactoryName = connectionFactoryName;
      return this;
    }

    public Builder setClientID(String clientID) {
      this.clientID = clientID;
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

    public Builder setProperty(String key, String value) {
      this.properties = MapUtils.addToMap(this.properties, key, value);
      return this;
    }

    public Builder setProperties(Map<String, String> properties) {
      this.properties = map(properties);
      return this;
    }
  }

}
