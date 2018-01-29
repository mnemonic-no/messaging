package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.component.Dependency;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.ClassLoaderContext;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.RequestSink;

import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A JMSRequestProxy is the listener component handling messages sent from a
 * {@link JMSRequestSink}, dispatching them to the configured {@link RequestSink}.
 * <p>
 * The JMSRequestProxy listens to messages on a JMS queue or topic, and will
 * unpack the message, signal the downstream RequestSink, and handle any replies.
 * <p>
 * Each request will be run in a separate thread, and the <code>maxConcurrentCalls</code> parameter
 * puts a limit on the maximum requests being handled. If more messages are sent to the JMS queue, these will
 * not be consumed by the JMS Request Sink until a thread is available.
 * This allows multiple JMSRequestProxies to share the load from a queue, and acts as a resource limitation.
 */
public class JMSRequestProxy extends JMSBase implements MessageListener, ExceptionListener {

  private static final Logger LOGGER = Logging.getLogger(JMSRequestProxy.class);

  static final int DEFAULT_MAX_CONCURRENT_CALLS = 10;

  // properties

  @Dependency
  private final RequestSink requestSink;

  // variables
  private final Map<String, ServerContext> calls = new ConcurrentHashMap<>();
  private final Semaphore semaphore;
  private final AtomicLong lastCleanupTimestamp = new AtomicLong();

  private final LongAdder totalCallsCounter = new LongAdder();
  private final LongAdder ignoredCallsCounter = new LongAdder();
  private final LongAdder errorCallsCounter = new LongAdder();
  private final ExecutorService executor;

  private final Set<JMSRequestProxyConnectionListener> connectionListeners = new HashSet<>();

  private JMSRequestProxy(String contextFactoryName, String contextURL, String connectionFactoryName,
                          String username, String password, Map<String, String> connectionProperties,
                          String destinationName, int priority, int maxConcurrentCalls,
                          int maxMessageSize, RequestSink requestSink) {
    super(contextFactoryName, contextURL, connectionFactoryName,
            username, password, connectionProperties, destinationName,
            priority, maxMessageSize);

    if (requestSink == null) throw new IllegalArgumentException("No requestSink provided");
    if (maxConcurrentCalls < 1)
      throw new IllegalArgumentException("maxConcurrentCalls cannot be lower than 1");
    this.requestSink = requestSink;
    this.executor = Executors.newFixedThreadPool(
            maxConcurrentCalls,
            new ThreadFactoryBuilder().setNamePrefix("JMSRequestProxy").build()
            );
    this.semaphore = new Semaphore(maxConcurrentCalls);
  }

  @Override
  public void startComponent() {
    try {
      getMessageConsumer().setMessageListener(this);
      SetUtils.set(connectionListeners).forEach(l -> l.connected(this));
    } catch (JMSException | NamingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void stopComponent() {
    try {
      //stop accepting messages
      getMessageConsumer().setMessageListener(null);
      //stop executor
      executor.shutdown();
      //wait for ongoing requests to finish
      LambdaUtils.tryTo(
              ()->executor.awaitTermination(10, TimeUnit.SECONDS),
              e->LOGGER.warning("Error waiting for executor termination")
      );
    } catch (Exception e) {
      LOGGER.warning(e, "Error stopping request proxy");
    }
    //now do cleanup of resources
    super.stopComponent();
  }

  public void onException(JMSException e) {
    errorCallsCounter.increment();
    super.onException(e);
  }

  @SuppressWarnings("WeakerAccess")
  public void addJMSRequestProxyConnectionListener(JMSRequestProxyConnectionListener listener) {
    this.connectionListeners.add(listener);
  }

  public void onMessage(javax.jms.Message message) {
    checkCleanRequests();
    process(message);
  }

  //private and protected methods

  /**
   * Processor method, handles an incoming message by forking up a new handler thread
   *
   * @param message message to process
   */
  private void process(javax.jms.Message message) {
    totalCallsCounter.increment();
    try {
      if (!JMSUtils.isCompatible(message)) {
        LOGGER.warning("Ignoring request of incompatible version: " + message);
        ignoredCallsCounter.increment();
        return;
      }

      long timeout = message.getLongProperty(PROPERTY_REQ_TIMEOUT);
      long maxWait = timeout - System.currentTimeMillis();
      if (maxWait <= 0) {
        LOGGER.warning("Ignoring request: timed out");
        ignoredCallsCounter.increment();
        return;
      }

      String messageType = message.getStringProperty(PROPERTY_MESSAGE_TYPE);
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< process [callID=%s type=%s]", message.getJMSCorrelationID(), messageType);
      }

      //avoid enqueueing a lot of messages into the executor queue, we rather want them to stay in JMS
      //if semaphore is depleted, this should block the activemq consumer, causing messages to queue up in JMS
      semaphore.acquire();
      executor.submit(() -> doProcessMessage(message, messageType, timeout));
    } catch (Exception e) {
      errorCallsCounter.increment();
      LOGGER.warning(e, "Error handling message");
    }
  }

  private void doProcessMessage(javax.jms.Message message, String messageType, long timeout) {
    try {
      // get reply address and call lifetime
      if (MESSAGE_TYPE_SIGNAL.equals(messageType)) {
        handleSignalMessage(message, timeout);
      } else if (MESSAGE_TYPE_CHANNEL_REQUEST.equals(messageType)) {
        handleChannelRequest(message, timeout);
      } else {
        ignoredCallsCounter.increment();
        LOGGER.warning("Ignoring unrecognized request type: " + messageType);
      }
    } catch (Exception e) {
      errorCallsCounter.increment();
      LOGGER.error(e, "Error handling JMS call");
    } finally {
      semaphore.release();
      if (LOGGER.isDebug()) {
        LOGGER.debug("# end process [type=%s]", messageType);
      }
    }
  }

  private void handleSignalMessage(javax.jms.Message message, long timeout) throws JMSException, NamingException {
    String callID = message.getJMSCorrelationID();
    Destination responseDestination = message.getJMSReplyTo();
    //ignore requests without a clear response destination/call ID
    if (callID == null || responseDestination == null) {
      if (LOGGER.isDebug())
        LOGGER.debug("Request without return information ignored: " + message);
      return;
    }
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< handleSignal [callID=%s]", message.getJMSCorrelationID());
    }
    // create a response context to handle response messages
    ServerResponseContext ctx = setupServerContext(callID, responseDestination, timeout, JMSUtils.getProtocolVersion(message));
    try (ClassLoaderContext ignored = ClassLoaderContext.of(requestSink)) {
      // requestsink will broadcast signal, and responses sent to response mockSink
      //use the classloader for the receiving sink when extracting object
      Message request = JMSUtils.extractObject(message);
      ctx.handle(requestSink, request);
    }
  }

  private void handleChannelRequest(javax.jms.Message message, long timeout) throws JMSException, NamingException {
    String callID = message.getJMSCorrelationID();
    Destination responseDestination = message.getJMSReplyTo();
    //ignore requests without a clear response destination/call ID
    if (callID == null || responseDestination == null) {
      LOGGER.info("Request without return information ignored: " + message);
      ignoredCallsCounter.increment();
      return;
    }
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< channelRequest [callID=%s]", message.getJMSCorrelationID());
    }
    setupChannel(callID, responseDestination, timeout, JMSUtils.getProtocolVersion(message));
  }

  private void handleChannelUploadCompleted(String callID, byte[] data, Destination replyTo, long timeout, ProtocolVersion protocolVersion) throws IOException, ClassNotFoundException, JMSException, NamingException {
    // create a response context to handle response messages
    ServerResponseContext r = new ServerResponseContext(callID, getSession(), replyTo, timeout, protocolVersion, getMaxMessageSize());
    // overwrite channel upload context with a server response context
    calls.put(callID, r);
    //send uploaded signal to requestSink
    try (ClassLoaderContext classLoaderCtx = ClassLoaderContext.of(requestSink)) {
      // requestsink will broadcast signal, and responses sent to response mockSink
      //use the classloader for the receiving sink when extracting object
      Message request = JMSUtils.unserialize(data, classLoaderCtx.getContextClassLoader());
      r.handle(requestSink, request);
    }
  }

  /**
   * Walk through responsesinks and remove them if they are closed
   */
  private void checkCleanRequests() {
    if (System.currentTimeMillis() - lastCleanupTimestamp.get() < 10000) return;
    lastCleanupTimestamp.set(System.currentTimeMillis());
    for (Map.Entry<String, ServerContext> e : calls.entrySet()) {
      ServerContext sink = e.getValue();
      if (sink != null && sink.isClosed()) {
        calls.remove(e.getKey());
      }
    }
  }

  /**
   * Create a response mockSink which will handle replies to the given callID, and send them to the given destination.
   * Sink will work for the given lifetime
   *
   * @param callID  callID for the call this responsesink is attached to
   * @param replyTo destination which responses will be sent to
   * @param timeout how long this responsesink will forward messages
   * @return a responsesink fulfilling this API
   */
  private ServerResponseContext setupServerContext(final String callID, Destination replyTo, long timeout, ProtocolVersion protocolVersion) throws JMSException, NamingException {
    ServerContext ctx = calls.get(callID);
    if (ctx != null) return (ServerResponseContext) ctx;
    //create new response context
    ServerResponseContext context = new ServerResponseContext(callID, getSession(), replyTo, timeout, protocolVersion, getMaxMessageSize());
    // register this responsesink
    calls.put(callID, context);
    // and return it
    return context;
  }

  private void setupChannel(String callID, Destination replyTo, long timeout, ProtocolVersion protocolVersion) throws NamingException, JMSException {
    ServerContext ctx = calls.get(callID);
    if (ctx != null) return;
    //create new upload context
    ServerChannelUploadContext context = new ServerChannelUploadContext(callID, getSession(), replyTo, timeout, protocolVersion);
    // register this responsesink
    calls.put(callID, context);
    //listen on upload messages and transmit channel setup
    context.setupChannel(this::handleChannelUploadCompleted);
  }

  //inner classes

  interface ServerContext {
    boolean isClosed();
  }

  //builder

  @SuppressWarnings("WeakerAccess")
  public static Builder builder() {
    return new Builder();
  }

  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class Builder extends JMSBase.BaseBuilder<Builder> {

    private RequestSink requestSink;
    private int maxConcurrentCalls = DEFAULT_MAX_CONCURRENT_CALLS;

    private Builder() {
    }

    //fields

    public JMSRequestProxy build() {
      return new JMSRequestProxy(contextFactoryName, contextURL, connectionFactoryName, username, password,
              connectionProperties, destinationName, priority, maxConcurrentCalls, maxMessageSize, requestSink);
    }

    //setters


    public Builder setMaxConcurrentCalls(int maxConcurrentCalls) {
      this.maxConcurrentCalls = maxConcurrentCalls;
      return this;
    }

    public Builder setRequestSink(RequestSink requestSink) {
      this.requestSink = requestSink;
      return this;
    }
  }

  //accessors

  public interface JMSRequestProxyConnectionListener {
    void connected(JMSRequestProxy proxy);
  }
}
