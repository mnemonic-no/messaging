package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.component.Dependency;
import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.AppendMembers;
import no.mnemonic.commons.utilities.AppendUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.MessagingException;

import javax.jms.*;
import javax.naming.NamingException;
import java.lang.IllegalStateException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;

@SuppressWarnings({"ClassReferencesSubclass"})
public abstract class JMSBase implements LifecycleAspect, AppendMembers {

  private static final Logger LOGGER = Logging.getLogger(JMSBase.class);

  static final String PROTOCOL_VERSION_KEY = "ArgusMessagingProtocol";


  // common properties

  @Dependency
  private final List<JMSConnection> connections;
  private final String destinationName;
  private final boolean transacted;
  private final boolean temporary;
  private final int timeToLive;
  private final int priority;
  private final boolean persistent;

  // variables

  private final LongAdder errorCount = new LongAdder();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicBoolean invalidating = new AtomicBoolean();
  private final AtomicInteger connectionPointer = new AtomicInteger();
  private final AtomicLong lastFailbackTime = new AtomicLong();

  private final AtomicReference<JMSConnection> activeConnection = new AtomicReference<>();
  private final AtomicReference<Session> session = new AtomicReference<>();
  private final AtomicReference<Message> latestMessage = new AtomicReference<>();
  private final AtomicReference<Destination> destination = new AtomicReference<>();
  private final AtomicReference<MessageConsumer> consumer = new AtomicReference<>();
  private final AtomicReference<MessageProducer> producer = new AtomicReference<>();
  private final AtomicReference<Thread> reconnectingThread = new AtomicReference<>();

  private final AtomicReference<ExecutorService> executor = new AtomicReference<>();


  // ************************* constructors ********************************

  JMSBase(List<JMSConnection> connections, String destinationName, boolean transacted,
          int timeToLive, int priority, boolean persistent, boolean temporary) {
    this.connections = connections;
    this.destinationName = destinationName;
    this.transacted = transacted;
    this.timeToLive = timeToLive;
    this.priority = priority;
    this.persistent = persistent;
    this.temporary = temporary;
  }

  // ************************ interface methods ***********************************


  @Override
  public void appendMembers(StringBuilder buf) {
    AppendUtils.appendField(buf, "destinationName", destinationName);
    AppendUtils.appendField(buf, "priority", priority);
    AppendUtils.appendField(buf, "timeToLive", timeToLive);
    AppendUtils.appendField(buf, "temporary", temporary);
    AppendUtils.appendField(buf, "persistent", persistent);
  }

  @Override
  public String toString() {
    return AppendUtils.toString(this);
  }

  @Override
  public void startComponent() {
    this.executor.set(createExecutor());
  }

  @Override
  public void stopComponent() {
    closed.set(true);
    closeAllResources();
    executor.get().shutdown();
    LambdaUtils.tryTo(() -> executor.get().awaitTermination(10, TimeUnit.SECONDS));
  }

  // other public methods

  public void invalidate() {
    //if another thread is doing reconnect, do not interfere!
    if (reconnectingThread.get() != null && reconnectingThread.get() != Thread.currentThread()) return;
    //if someone else is already invalidating, leave it
    if (!invalidating.compareAndSet(false, true)) return;
    try {
      //don`t do invalidation if not connected
      LOGGER.info("Invalidating client: %s (%s)", getClass().getSimpleName(), getDestinationName());
      closeAllResources();
    } finally {
      invalidating.set(false);
    }
  }

  public boolean isClosed() {
    return closed.get();
  }

  // ******************** protected and private methods ***********************

  ExecutorService createExecutor() {
    return Executors.newSingleThreadExecutor();
  }

  private void closeAllResources() {
    try {
      // try to nicely shut down all resources
      executeAndReset(activeConnection, c -> c.deregister(this), "Error deregistering connection");
      executeAndReset(consumer, MessageConsumer::close, "Error closing consumer");
      executeAndReset(producer, MessageProducer::close, "Error closing producer");
      executeAndReset(session, Session::close, "Error closing session");
    } finally {
      resetState();
    }
  }

  private <T> void executeAndReset(AtomicReference<T> ref, JMSConsumer<T> op, String errorString) {
    T val = ref.getAndUpdate(t -> null);
    if (val != null) {
      try {
        op.apply(val);
      } catch (Exception e) {
        LOGGER.warning(e, errorString);
      }
    }
  }

  void doReconnect(long maxReconnectTime, MessageListener messageListener, ExceptionListener exceptionListener) {
    //if someone else is already reconnecting, drop this
    if (!reconnectingThread.compareAndSet(null, Thread.currentThread())) return;
    try {
      long timeout = System.currentTimeMillis() + maxReconnectTime;
      //retry until successful or timeout occurs (or component is shut down
      while (!isClosed() && System.currentTimeMillis() < timeout) {
        try {
          getMessageConsumer().setMessageListener(messageListener);
          getConnection().addExceptionListener(exceptionListener);
          LOGGER.info("Reconnected JMS proxy to %s/%s", getConnection(), getDestinationName());
          return;
        } catch (Exception ex) {
          LOGGER.error(ex, "Error in reconnect");
          closeAllResources();
        }
        //sleep between attempts
        try {
          Thread.sleep(1000);
        } catch (Exception ignored) {
        }
      }
      //no luck in reconnect, shut down
      LOGGER.warning("Timeout in reconnect(), shutting down");
      stopComponent();
    } finally {
      reconnectingThread.set(null);
    }
  }

  void runInSeparateThread(Runnable task) {
    //noinspection unchecked
    if (executor.get() == null) throw new IllegalStateException("Component not started");
    executor.get().submit(task);
  }

  void invalidateInSeparateThread() {
    //if someone else is already invalidating, skip it
    if (isInvalidating()) return;
    runInSeparateThread(() -> {
      try {
        invalidate();
      } catch (Exception e) {
        LOGGER.error(e, "Error in invalidate");
      }
    });
  }

  private void resetState() {
    session.set(null);
    destination.set(null);
    latestMessage.set(null);
    consumer.set(null);
    producer.set(null);
    activeConnection.set(null);
  }

  JMSConnection getConnection() throws JMSException, NamingException {
    synchronized (this) {
      if (activeConnection.get() != null) {
        return activeConnection.get();
      }
      if (connections == null || connections.size() == 0) {
        throw new JMSException("No JMS connections defined");
      }
      activeConnection.set(connections.get(connectionPointer.get()));
      connectionPointer.set((connectionPointer.get() + 1) % connections.size());
      //start failback timer now
      lastFailbackTime.set(System.currentTimeMillis());
      LOGGER.info("Using connection %s", activeConnection.get());
      return activeConnection.get();
    }
  }

  Destination getDestination() throws JMSException, NamingException {
    if (isClosed()) throw new RuntimeException("closed");
    return getOrUpdateSynchronized(destination, () -> getConnection().lookupDestination(destinationName));
  }

  Session getSession() throws JMSException, NamingException {
    if (isClosed()) throw new RuntimeException("closed");
    return getOrUpdateSynchronized(session, () -> getConnection().getSession(this, transacted));
  }

  MessageConsumer getMessageConsumer() throws JMSException, NamingException {
    if (isClosed()) throw new RuntimeException("closed");
    return getOrUpdateSynchronized(consumer, () -> getSession().createConsumer(getDestination()));
  }

  private MessageProducer getMessageProducer() throws JMSException, NamingException {
    if (isClosed()) throw new RuntimeException("closed");
    return getOrUpdateSynchronized(producer, () -> getSession().createProducer(getDestination()));
  }

  private <U> U getOrUpdateSynchronized(AtomicReference<U> ref, JMSSupplier<U> task) throws NamingException, JMSException {
    try {
      return ref.updateAndGet(t -> {
        if (t != null) return t;
        try {
          return task.get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    } catch (RuntimeException e) {
      if (e.getCause() instanceof JMSException) throw (JMSException) e.getCause();
      if (e.getCause() instanceof NamingException) throw (NamingException) e.getCause();
      throw e;
    }
  }

  private interface JMSSupplier<T> {
    T get() throws JMSException, NamingException;
  }

  private interface JMSConsumer<T> {
    void apply(T val) throws JMSException, NamingException;
  }

  /**
   * Send JMS message to producer, using this objects settings
   *
   * @param msg message to send
   */
  void sendJMSMessage(final Message msg) {
    try {
      getMessageProducer().send(msg, (isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT),
              getPriority(), getTimeToLive());
    } catch (Exception e) {
      throw handleMessagingException(e);
    }
  }

  MessagingException handleMessagingException(Exception e) {
    if (!temporary) {
      invalidate();
      errorCount.increment();
    }
    throw new MessagingException(e);
  }

  // ************************* property accessors ********************************

  public boolean hasSession() {
    return session.get() != null;
  }

  public boolean isInvalidating() {
    return invalidating.get();
  }

  public boolean isReconnecting() {
    return reconnectingThread.get() != null;
  }

  public boolean isPersistent() {
    return persistent;
  }

  public int getPriority() {
    return priority;
  }

  public int getTimeToLive() {
    return timeToLive;
  }

  public JMSConnection getActiveConnection() {
    return activeConnection.get();
  }

  public String getDestinationName() {
    return destinationName;
  }

  public boolean isTransacted() {
    return transacted;
  }
}
