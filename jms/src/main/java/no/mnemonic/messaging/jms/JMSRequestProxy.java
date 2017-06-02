package no.mnemonic.messaging.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.messaging.api.Message;
import no.mnemonic.messaging.api.RequestSink;

import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.naming.NamingException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class JMSRequestProxy extends JMSBase implements MessageListener, ExceptionListener {

  private static final Logger LOGGER = Logging.getLogger(JMSRequestProxy.class);

  static final String PROPERTY_MESSAGE_TYPE = "MessageType";
  static final String MESSAGE_TYPE_STREAM_CLOSED = "JMSStreamClosed";
  static final String MESSAGE_TYPE_EXCEPTION = "JMSException";
  static final String MESSAGE_TYPE_SIGNAL = "JMSSignal";
  static final String MESSAGE_TYPE_CHANNEL_REQUEST = "JMSChannelRequest";
  static final String MESSAGE_TYPE_CHANNEL_SETUP = "JMSChannelSetup";
  static final String MESSAGE_TYPE_SIGNAL_FRAGMENT = "JMSSignalFragment";
  static final String MESSAGE_TYPE_SIGNAL_RESPONSE = "JMSSignalResponse";
  static final String MESSAGE_TYPE_EXTEND_WAIT = "JMSExtendWait";
  static final String PROPERTY_REQ_TIMEOUT = "RequestTimeout";
  static final String PROPERTY_FRAGMENTS_TOTAL = "TotalFragments";
  static final String PROPERTY_FRAGMENTS_IDX = "FragmentIndex";
  static final String PROPERTY_DATA_CHECKSUM_MD5 = "DataChecksumMD5";

  static final int DEFAULT_MAX_CONCURRENT_CALLS = 10;
  static final int DEFAULT_MAX_RECONNECT_TIME = 3600000;
  static final int DEFAULT_TTL = 1000;
  static final int DEFAULT_FAILBACK_INTERVAL = 300000;
  static final int DEFAULT_PRIORITY = 1;
  static final int MINIMUM_RECONNECT_TIME = 1000;

  // properties
  private final long maxReconnectTime;
  private final RequestSink requestSink;

  // variables
  private final Map<String, ServerContext> calls = new ConcurrentHashMap<>();
  private final Semaphore semaphore;
  private final AtomicLong lastCleanupTimestamp = new AtomicLong();

  private final LongAdder totalCallsCounter = new LongAdder();
  private final LongAdder ignoredCallsCounter = new LongAdder();
  private final LongAdder errorCallsCounter = new LongAdder();
  private final LongAdder reconnectCounter = new LongAdder();

  private final Set<JMSRequestProxyConnectionListener> connectionListeners = new HashSet<>();

  private JMSRequestProxy(List<JMSConnection> connections, String destinationName, long failbackInterval,
                          int timeToLive, int priority, int maxConcurrentCalls,
                          long maxReconnectTime, RequestSink requestSink) {
    super(connections, destinationName, false, failbackInterval, timeToLive,
            priority, false, false,
            Executors.newFixedThreadPool(maxConcurrentCalls)
    );
    this.maxReconnectTime = maxReconnectTime;
    this.requestSink = requestSink;
    this.semaphore = new Semaphore(maxConcurrentCalls);
  }

  @Override
  public void startComponent() {
    super.startComponent();
    reconnectInSeparateThread();
  }

  @Override
  public void invalidate() {
    try {
      super.invalidate();
    } catch (Exception e) {
      LOGGER.error(e, "Error in invalidate()");
    }
    reconnectInSeparateThread();
  }

  public void onException(JMSException e) {
    //tear down connection
    errorCallsCounter.increment();
    LOGGER.error(e, "Caught error");
    invalidateInSeparateThread();
  }

  @SuppressWarnings("WeakerAccess")
  public void addJMSRequestProxyConnectionListener(JMSRequestProxyConnectionListener listener) {
    this.connectionListeners.add(listener);
  }

  public void onMessage(javax.jms.Message message) {
    checkCleanRequests();
    process(message);
  }

  //private and protected methods methods

  private void reconnectInSeparateThread() {
    if (isReconnecting()) return;
    runInSeparateThread(() -> doReconnect(maxReconnectTime, this, this));
  }

  @Override
  void doReconnect(long maxReconnectTime, MessageListener messageListener, ExceptionListener exceptionListener) {
    reconnectCounter.increment();
    super.doReconnect(maxReconnectTime, messageListener, exceptionListener);
    SetUtils.set(connectionListeners).forEach(l -> l.connected(this));
  }

  /**
   * Processor method, handles an incoming message by forking up a new handler thread
   *
   * @param message message to process
   */
  private void process(final javax.jms.Message message) {
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

      //avoid enqueueing a lot of messages into the executor queue, we rather want them to stay in JMS
      //if semaphore is depleted, this should block the activemq consumer, causing messages to queue up in JMS
      semaphore.acquire();
      try {
        runInSeparateThread(() -> doProcessMessage(message, timeout));
      } finally {
        semaphore.release();
      }
    } catch (Throwable e) {
      errorCallsCounter.increment();
      LOGGER.warning(e, "Error handling message");
    }
  }

  private void doProcessMessage(javax.jms.Message message, long timeout) {
    try {
      // get reply address and call lifetime
      String messageType = message.getStringProperty(PROPERTY_MESSAGE_TYPE);
      if (MESSAGE_TYPE_SIGNAL.equals(messageType)) {
        handleSignalMessage(message, timeout);
      } else if (MESSAGE_TYPE_CHANNEL_REQUEST.equals(messageType)) {
        handleChannelRequest(message, timeout);
      } else {
        ignoredCallsCounter.increment();
        LOGGER.warning("Ignoring unrecognized request type: " + messageType);
      }
    } catch (Throwable e) {
      errorCallsCounter.increment();
      LOGGER.error(e, "Error handling JMS call");
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
    // create a response context to handle response messages
    ServerResponseContext ctx = setupServerContext(callID, responseDestination, timeout, JMSUtils.getProtocolVersion(message));
    try (JMSUtils.ClassLoaderContext ignored = JMSUtils.ClassLoaderContext.of(requestSink)) {
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
      if (LOGGER.isDebug())
        LOGGER.debug("Request without return information ignored: " + message);
      return;
    }
    setupChannel(callID, responseDestination, timeout);
  }

  private void handleChannelUploadCompleted(String callID, byte[] data, Destination replyTo, long timeout) throws Exception {
    // create a response context to handle response messages
    ServerResponseContext r = new ServerResponseContext(callID, getSession(), replyTo, timeout, JMSUtils.ProtocolVersion.V16);
    // overwrite channel upload context with a server response context
    calls.put(callID, r);
    //send uploaded signal to requestSink
    try (JMSUtils.ClassLoaderContext classLoaderCtx = JMSUtils.ClassLoaderContext.of(requestSink)) {
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
  private ServerResponseContext setupServerContext(final String callID, Destination replyTo, long timeout, JMSUtils.ProtocolVersion protocolVersion) throws JMSException, NamingException {
    ServerContext ctx = calls.get(callID);
    if (ctx != null) return (ServerResponseContext) ctx;
    //create new response context
    ServerResponseContext context = new ServerResponseContext(callID, getSession(), replyTo, timeout, protocolVersion);
    // register this responsesink
    calls.put(callID, context);
    // and return it
    return context;
  }

  private void setupChannel(String callID, Destination replyTo, long timeout) throws NamingException, JMSException {
    ServerContext ctx = calls.get(callID);
    if (ctx != null) return;
    //create new upload context
    ServerChannelUploadContext context = new ServerChannelUploadContext(callID, getSession(), replyTo, timeout);
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
  public static class Builder {

    private List<JMSConnection> connections;
    private RequestSink requestSink;
    private String destinationName;
    private long failbackInterval = DEFAULT_FAILBACK_INTERVAL;
    private int timeToLive = DEFAULT_TTL;
    private int priority = DEFAULT_PRIORITY;
    private int maxConcurrentCalls = DEFAULT_MAX_CONCURRENT_CALLS;
    private long maxReconnectTime = DEFAULT_MAX_RECONNECT_TIME; //1 hour default;

    //fields

    JMSRequestProxy build() {
      if (CollectionUtils.isEmpty(connections)) throw new IllegalArgumentException("No connections defined");
      if (requestSink == null) throw new IllegalArgumentException("No requestSink provided");
      if (destinationName == null) throw new IllegalArgumentException("No destination name provided");
      if (maxReconnectTime < MINIMUM_RECONNECT_TIME)
        throw new IllegalArgumentException("Invalid value for maxReconnectTime, proxy will not be able to connect");
      return new JMSRequestProxy(connections, destinationName, failbackInterval, timeToLive, priority, maxConcurrentCalls, maxReconnectTime, requestSink);
    }

    //setters

    public Builder setMaxConcurrentCalls(int maxConcurrentCalls) {
      this.maxConcurrentCalls = maxConcurrentCalls;
      return this;
    }

    public Builder addConnection(JMSConnection c) {
      this.connections = ListUtils.addToList(this.connections, c);
      return this;
    }

    public Builder setConnections(List<JMSConnection> connections) {
      this.connections = connections;
      return this;
    }

    public Builder setRequestSink(RequestSink requestSink) {
      this.requestSink = requestSink;
      return this;
    }

    public Builder setDestinationName(String destinationName) {
      this.destinationName = destinationName;
      return this;
    }

    public Builder setFailbackInterval(long failbackInterval) {
      this.failbackInterval = failbackInterval;
      return this;
    }

    public Builder setTimeToLive(int timeToLive) {
      this.timeToLive = timeToLive;
      return this;
    }

    public Builder setPriority(int priority) {
      this.priority = priority;
      return this;
    }

    public Builder setMaxReconnectTime(long maxReconnectTime) {
      this.maxReconnectTime = maxReconnectTime;
      return this;
    }
  }


  //accessors

  public interface JMSRequestProxyConnectionListener {
    void connected(JMSRequestProxy proxy);
  }
}
