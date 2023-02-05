package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.component.Dependency;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.*;
import no.mnemonic.commons.utilities.ClassLoaderContext;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.jms.context.ServerChannelUploadContext;
import no.mnemonic.messaging.requestsink.jms.context.ServerContext;
import no.mnemonic.messaging.requestsink.jms.context.ServerResponseContext;
import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import no.mnemonic.messaging.requestsink.jms.serializer.MessageSerializer;
import no.mnemonic.messaging.requestsink.jms.util.ServerMetrics;
import no.mnemonic.messaging.requestsink.jms.util.ThreadFactoryBuilder;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNull;
import static no.mnemonic.commons.utilities.collections.SetUtils.set;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.*;

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
public class JMSRequestProxy extends AbstractJMSRequestBase implements MetricAspect {

  private static final Logger LOGGER = Logging.getLogger(JMSRequestProxy.class);

  static final int DEFAULT_MAX_CONCURRENT_CALLS = 10;
  static final int DEFAULT_SHUTDOWN_TIMEOUT = 10000;

  // properties

  @Dependency
  private final RequestSink requestSink;

  // variables
  private final Map<String, ServerContext> calls = new ConcurrentHashMap<>();
  private final Semaphore semaphore;
  private final AtomicLong lastCleanupTimestamp = new AtomicLong();

  private final ServerMetrics metrics = new ServerMetrics();
  private final LongAdder awaitPermitTime = new LongAdder();

  private final ExecutorService executor;
  private final long shutdownTimeout;

  private final Set<JMSRequestProxyConnectionListener> connectionListeners = new HashSet<>();
  private final Map<String, MessageSerializer> serializers;
  private final AtomicReference<MessageProducer> replyProducer = new AtomicReference<>();
  private final AtomicReference<MessageConsumer> queueConsumer = new AtomicReference<>();
  private final AtomicReference<MessageConsumer> topicConsumer = new AtomicReference<>();


  private JMSRequestProxy(String contextFactoryName, String contextURL, String connectionFactoryName,
                          String username, String password, Map<String, String> connectionProperties,
                          String queueName, String topicName, int priority, int maxConcurrentCalls,
                          int maxMessageSize, RequestSink requestSink, long shutdownTimeout,
                          Collection<MessageSerializer> serializers) {
    super(contextFactoryName, contextURL, connectionFactoryName,
        username, password, connectionProperties,
        queueName, topicName,
        priority, maxMessageSize);

    if (maxConcurrentCalls < 1)
      throw new IllegalArgumentException("maxConcurrentCalls cannot be lower than 1");

    this.shutdownTimeout = shutdownTimeout;
    this.serializers = configureSerializersWithDefault(serializers);
    this.requestSink = assertNotNull(requestSink, "requestSink not set");
    this.executor = Executors.newFixedThreadPool(
        maxConcurrentCalls,
        new ThreadFactoryBuilder().setNamePrefix("JMSRequestProxy").build()
    );
    this.semaphore = new Semaphore(maxConcurrentCalls);
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    MetricsGroup m = new MetricsGroup();
    // Add all server metrics.
    m.addSubMetrics("server", metrics.metrics());
    m.addSubMetrics("requests", new MetricsData()
        .addData("runningRequests", calls.size()) // Approximation as finished contexts are counted until they are cleaned up.
        .addData("pendingRequests", semaphore.getQueueLength())
        .addData("availablePermits", semaphore.availablePermits())
        .addData("awaitPermitTime", awaitPermitTime)
    );
    // Add metrics for all serializers.
    for (MessageSerializer serializer : serializers.values()) {
      m.addSubMetrics(serializer.serializerID(), serializer.getMetrics());
    }

    return m;
  }

  @Override
  public void startComponent() {
    try {
      connect();
    } catch (Exception e) {
      executor.shutdown();
      throw new IllegalStateException(e);
    }
  }


  @Override
  public void stopComponent() {
    try {
      //stop accepting messages
      ifNotNull(
          queueConsumer.get(),
          c -> tryTo(() -> c.setMessageListener(null), e -> LOGGER.warning(e, "Error removing message listener"))
      );
      //stop accepting requests
      closed.set(true);
      //stop executor
      executor.shutdown();
      //wait for ongoing requests to finish
      tryTo(
          () -> executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS),
          e -> LOGGER.warning("Error waiting for executor termination")
      );
    } catch (Exception e) {
      LOGGER.warning(e, "Error stopping request proxy");
    }
    //now do cleanup of resources
    closeAllResources();
  }

  private void connect() throws NamingException, JMSException {
    synchronized (this) {
      MessageProducer p = getSession().createProducer(null);
      p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      replyProducer.set(p);

      MessageConsumer qc = getSession().createConsumer(getQueue());
      qc.setMessageListener(this::processQueueMessage);
      queueConsumer.set(qc);

      Optional<Topic> optionalTopic = getTopic();
      if (optionalTopic.isPresent()) {
        MessageConsumer tc = getSession().createConsumer(optionalTopic.get());
        tc.setMessageListener(this::processTopicMessage);
      }

      set(connectionListeners).forEach(l -> l.connected(this));
      LOGGER.info("Connected");
      metrics.reconnected();
    }
  }

  private synchronized void closeAllResources() {
    try {
      // try to nicely shut down all resources
      executeAndReset(replyProducer, MessageProducer::close, "Error closing reply producer");
      executeAndReset(queueConsumer, MessageConsumer::close, "Error closing queue consumer");
      executeAndReset(topicConsumer, MessageConsumer::close, "Error closing topic consumer");
      executeAndReset(session, Session::close, "Error closing session");
      executeAndReset(connection, Connection::close, "Error closing connection");
    } finally {
      resetState();
    }
  }

  private synchronized void resetState() {
    replyProducer.set(null);
    queueConsumer.set(null);
    topicConsumer.set(null);
    session.set(null);
    queue.set(null);
    topic.set(null);
  }

  @Override
  public void onException(JMSException e) {
    metrics.error();
    LOGGER.warning(e, "Exception received");
  }

  @SuppressWarnings("WeakerAccess")
  public void addJMSRequestProxyConnectionListener(JMSRequestProxyConnectionListener listener) {
    this.connectionListeners.add(listener);
  }

  //private and protected methods

  /**
   * Processor method, handles an incoming message by forking up a new handler thread
   *
   * @param message message to process
   */
  private void processQueueMessage(javax.jms.Message message) {
    metrics.request();
    checkCleanRequests();
    try {
      if (!isCompatible(message)) {
        LOGGER.warning("Ignoring queue request of incompatible version: " + message);
        metrics.incompatibleMessage();
        return;
      }

      long timeout = message.getLongProperty(PROPERTY_REQ_TIMEOUT);
      long maxWait = timeout - System.currentTimeMillis();
      if (maxWait <= 0) {
        LOGGER.warning("Ignoring queue request: timed out");
        metrics.requestTimeout();
        return;
      }

      String messageType = message.getStringProperty(PROPERTY_MESSAGE_TYPE);
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< process queueMessage [callID=%s type=%s]", message.getJMSCorrelationID(), messageType);
      }

      try (TimerContext ignored = TimerContext.timerMillis(awaitPermitTime::add)) {
        //avoid enqueueing a lot of messages into the executor queue, we rather want them to stay in JMS
        //if semaphore is depleted, this should block the activemq consumer, causing messages to queue up in JMS
        semaphore.acquire();
      }

      //schedule message for processing
      executor.submit(() -> doProcessQueueMessage(message, messageType, timeout));
    } catch (Exception e) {
      metrics.error();
      LOGGER.warning(e, "Error handling message");
    }
  }

  private void doProcessQueueMessage(javax.jms.Message message, String messageType, long timeout) {
    try {
      // get reply address and call lifetime
      if (MESSAGE_TYPE_SIGNAL.equals(messageType)) {
        handleSignalMessage(message, timeout);
      } else if (MESSAGE_TYPE_CHANNEL_REQUEST.equals(messageType)) {
        handleChannelRequest(message, timeout);
      } else {
        metrics.incompatibleMessage();
        LOGGER.warning("Ignoring unrecognized queue request type: " + messageType);
      }
    } catch (Exception e) {
      metrics.error();
      LOGGER.error(e, "Error handling JMS call");
    } finally {
      semaphore.release();
      if (LOGGER.isDebug()) {
        LOGGER.debug("# end process [type=%s]", messageType);
      }
    }
  }

  private void processTopicMessage(javax.jms.Message message) {
    metrics.request();
    try {
      if (!isCompatible(message)) {
        LOGGER.warning("Ignoring topic request of incompatible version: " + message);
        metrics.incompatibleMessage();
        return;
      }

      String messageType = message.getStringProperty(PROPERTY_MESSAGE_TYPE);
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< process topicMessage [callID=%s type=%s]", message.getJMSCorrelationID(), messageType);
      }

      //schedule topic message for direct processing
      //
      //not waiting for a semaphore for topic messages means that these will be processed even if
      //queue messages are throttled by semaphore.
      executor.submit(() -> doProcessTopicMessage(message, messageType));
    } catch (Exception e) {
      metrics.error();
      LOGGER.warning(e, "Error handling message");
    }
  }

  private void doProcessTopicMessage(javax.jms.Message message, String messageType) {
    try {
      if (MESSAGE_TYPE_STREAM_CLOSED.equals(messageType)) {
        handleClientClosedStream(message);
      } else {
        metrics.incompatibleMessage();
        LOGGER.warning("Ignoring unrecognized topic request type: " + messageType);
      }
    } catch (Exception e) {
      metrics.error();
      LOGGER.error(e, "Error handling JMS call");
    } finally {
      if (LOGGER.isDebug()) {
        LOGGER.debug("# end process [type=%s]", messageType);
      }
    }
  }

  private void handleSignalMessage(javax.jms.Message message, long timeout) throws JMSException, NamingException {
    String callID = message.getJMSCorrelationID();
    MessageSerializer serializer = determineSerializer(message, pickLegacySerializer(), serializers);
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
    ServerResponseContext ctx = setupServerContext(callID, responseDestination, timeout, getProtocolVersion(message), serializer);
    try {
      ctx.handle(serializer.deserialize(extractMessageBytes(message), Thread.currentThread().getContextClassLoader()));
    } catch (IOException e) {
      LOGGER.error(e, "Illegal deserialization reading message from client");
      ctx.notifyError(e);
      ctx.endOfStream();
    }
  }

  private void handleChannelRequest(javax.jms.Message message, long timeout) throws JMSException, NamingException {
    String callID = message.getJMSCorrelationID();
    MessageSerializer serializer = determineSerializer(message, pickLegacySerializer(), serializers);
    Destination responseDestination = message.getJMSReplyTo();
    //ignore requests without a clear response destination/call ID
    if (callID == null || responseDestination == null) {
      LOGGER.info("Request without return information ignored: " + message);
      metrics.incompatibleMessage();
      return;
    }
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< channelRequest [callID=%s]", message.getJMSCorrelationID());
    }
    setupChannel(callID, responseDestination, timeout, getProtocolVersion(message), serializer);
  }

  private void handleClientClosedStream(javax.jms.Message message) throws JMSException {
    String callID = message.getJMSCorrelationID();
    if (callID == null) {
      LOGGER.info("Request without callID ignored: " + message);
      metrics.incompatibleMessage();
      return;
    }
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< clientClosedStream [callID=%s]", message.getJMSCorrelationID());
    }
    ServerContext serverContext = calls.get(callID);
    if (serverContext == null) {
      LOGGER.info("Request to abort unknown callID : " + callID);
      return;
    }
    serverContext.abort();
  }

  private void handleChannelUploadCompleted(String callID, byte[] data, Destination replyTo, long timeout, ProtocolVersion protocolVersion, MessageSerializer serializer) throws IOException, JMSException, NamingException {
    // create a response context to handle response messages
    ServerResponseContext r = new ServerResponseContext(callID, getSession(), replyProducer.get(), replyTo, timeout, protocolVersion, getMaxMessageSize(), metrics, serializer, requestSink);
    // overwrite channel upload context with a server response context
    calls.put(callID, r);
    //send uploaded signal to requestSink
    try (ClassLoaderContext classLoaderCtx = ClassLoaderContext.of(requestSink)) {
      // requestsink will broadcast signal, and responses sent to response mockSink
      //use the classloader for the receiving sink when extracting object
      Message request = serializer.deserialize(data, classLoaderCtx.getContextClassLoader());
      metrics.fragmentedUploadCompleted();
      r.handle(request);
    } catch (IllegalDeserializationException e) {
      LOGGER.error(e, "Illegal deserialization reading fragmented client message");
      r.notifyError(e);
      r.endOfStream();
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
  private ServerResponseContext setupServerContext(final String callID, Destination replyTo, long timeout, ProtocolVersion protocolVersion, MessageSerializer serializer) throws JMSException, NamingException {
    ServerContext ctx = calls.get(callID);
    if (ctx != null) return (ServerResponseContext) ctx;
    //create new response context
    ServerResponseContext context = new ServerResponseContext(callID, getSession(), replyProducer.get(), replyTo, timeout, protocolVersion, getMaxMessageSize(), metrics, serializer, requestSink);
    // register this responsesink
    calls.put(callID, context);
    // and return it
    return context;
  }

  private void setupChannel(String callID, Destination replyTo, long timeout, ProtocolVersion protocolVersion, MessageSerializer serializer) throws NamingException, JMSException {
    metrics.fragmentedUploadRequested();
    ServerContext ctx = calls.get(callID);
    if (ctx != null) return;
    //create new upload context
    ServerChannelUploadContext context = new ServerChannelUploadContext(callID, getSession(), replyTo, timeout, protocolVersion, metrics, serializer);
    // register this responsesink
    calls.put(callID, context);
    //listen on upload messages and transmit channel setup
    context.setupChannel(this::handleChannelUploadCompleted);
  }

  private Map<String, MessageSerializer> configureSerializersWithDefault(Collection<MessageSerializer> serializers) {
    if (CollectionUtils.isEmpty(serializers)) serializers = new ArrayList<>();

    // Server must support at least Java serialization for Protocol version < V3.
    if (serializers.stream().noneMatch(ser -> ser instanceof DefaultJavaMessageSerializer)) {
      serializers.add(new DefaultJavaMessageSerializer());
    }

    return MapUtils.map(serializers, ser -> MapUtils.pair(ser.serializerID(), ser));
  }

  private MessageSerializer pickLegacySerializer() {
    return this.serializers.values()
        .stream()
        .filter(ser -> ser instanceof DefaultJavaMessageSerializer)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Default serializer not configured"));
  }

  //builder

  @SuppressWarnings("WeakerAccess")
  public static Builder builder() {
    return new Builder();
  }

  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class Builder extends AbstractJMSRequestBase.BaseBuilder<Builder> {

    private RequestSink requestSink;
    private int maxConcurrentCalls = DEFAULT_MAX_CONCURRENT_CALLS;
    private int shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;
    private List<MessageSerializer> serializers = ListUtils.list();

    private Builder() {
    }

    //fields

    public JMSRequestProxy build() {
      return new JMSRequestProxy(
          contextFactoryName, contextURL, connectionFactoryName, username, password,
          connectionProperties, queueName, topicName, priority,
          maxConcurrentCalls, maxMessageSize, requestSink, shutdownTimeout, serializers);
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

    public Builder setSerializers(Collection<MessageSerializer> serializers) {
      this.serializers = ListUtils.list(serializers);
      return this;
    }

    public Builder addSerializer(MessageSerializer serializer) {
      this.serializers = ListUtils.addToList(this.serializers, serializer);
      return this;
    }

    public Builder setShutdownTimeout(int shutdownTimeout) {
      this.shutdownTimeout = shutdownTimeout;
      return this;
    }
  }

  //accessors

  public interface JMSRequestProxyConnectionListener {
    void connected(JMSRequestProxy proxy);
  }

}
