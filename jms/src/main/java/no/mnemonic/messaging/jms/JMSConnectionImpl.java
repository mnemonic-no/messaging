package no.mnemonic.messaging.jms;

import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.AppendMembers;
import no.mnemonic.commons.utilities.AppendUtils;
import no.mnemonic.commons.utilities.StringUtils;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.api.MessagingException;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
  private final ExecutorService executor = Executors.newSingleThreadExecutor();


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
    MapUtils.concatenate(properties).forEach(p::setProperty);
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

  }

  @Override
  public void stopComponent() {
    closed.set(true);
    executor.shutdown();
  }


  // public methods

  public boolean isClosed() {
    return closed.get();
  }

  public void onException(JMSException e) {
    if (invalidating.get()) return;
    exceptionListeners.forEach(l -> LambdaUtils.tryTo(() -> l.onException(e)));
  }

  public void addExceptionListener(ExceptionListener listener) {
    exceptionListeners.add(listener);
  }

  public void removeExceptionListener(ExceptionListener listener) {
    exceptionListeners.remove(listener);
  }

  public Destination lookupDestination(String destinationName) throws JMSException, NamingException {
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
    if (connection.get() == null) {
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
      Connection conn;
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
    }
    return connection.get();
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
        MapUtils.concatenate(properties).forEach((k, v) -> env.put((String) k, (String) v));
        initialContext.set(new InitialContext(env));
      }
      return initialContext.get();
    }
  }

  public void register(JMSBase client) {
    if (LOGGER.isDebug()) LOGGER.debug("Registering JMS client");
    synchronized (this) {
      if (!clients.contains(client))
        clients.add(client);
    }
  }

  public void deregister(JMSBase client) {
    if (LOGGER.isDebug()) LOGGER.debug("Deregistering JMS client");
    synchronized (this) {
      if (clients.contains(client)) {
        clients.remove(client);
      }
      if (client instanceof ExceptionListener) {
        exceptionListeners.remove(client);
      }
    }
  }

  public void invalidate() {
    // avoid multiple calls to this method
    if (invalidating.get())
      return;
    invalidating.set(true);

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
      try {
        if (connection.get() != null)
          connection.get().close();
      } catch (JMSException e) {
        //do nothing
      }
      try {
        if (initialContext.get() != null)
          initialContext.get().close();
      } catch (NamingException e) {
        //do nothing
      }
      connection.set(null);
      initialContext.set(null);
      invalidating.set(false);
    }
  }

  public void close() {
    invalidate();
  }

  // private methods

  private void invalidateInSeparateThread() {
    executor.submit(this::invalidate);
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
      this.properties = properties;
      return this;
    }
  }

}
