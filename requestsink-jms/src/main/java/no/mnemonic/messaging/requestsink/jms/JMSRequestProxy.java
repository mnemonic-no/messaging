package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.component.Dependency;
import no.mnemonic.commons.logging.LocalLoggingContext;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.MetricsGroup;
import no.mnemonic.commons.metrics.TimerContext;
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

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.naming.NamingException;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

  private static final int DEFAULT_MAX_CONCURRENT_CALLS_STANDARD = 5;
  private static final int DEFAULT_MAX_CONCURRENT_CALLS_BULK = 5;
  private static final int DEFAULT_MAX_CONCURRENT_CALLS_EXPEDITE = 2;
  private static final int DEFAULT_SHUTDOWN_TIMEOUT = 10000;
  private static final int DEFAULT_MINIMUM_STANDARD_CAPACITY = 3;
  private static final String LOG_KEY_PRIORITY = "priority";

  private static final Clock clock = Clock.systemUTC();

  // properties

  @Dependency
  private final RequestSink requestSink;

  // variables
  private final Map<String, ServerContext> calls = new ConcurrentHashMap<>();
  private final AtomicLong lastCleanupTimestamp = new AtomicLong();

  private final ServerMetrics metrics = new ServerMetrics();
  private final LongAdder awaitPermitTime = new LongAdder();

  private final ExecutorService executor;
  private final Semaphore bulkSemaphore;
  private final Semaphore standardSemaphore;
  private final Semaphore expediteSemaphore;
  private final ConcurrentMap<Message.Priority, LongAdder> requestCounters = new ConcurrentHashMap<>();
  private final ConcurrentMap<Message.Priority, LongAdder> rejectCounters = new ConcurrentHashMap<>();
  private final long shutdownTimeout;

  private final Set<JMSRequestProxyConnectionListener> connectionListeners = new HashSet<>();
  private final Set<Session> consumerSessions = Collections.synchronizedSet(new HashSet<>());
  private final Set<MessageConsumer> allConsumers = Collections.synchronizedSet(new HashSet<>());
  private final Map<String, MessageSerializer> serializers;
  private final AtomicReference<MessageProducer> replyProducer = new AtomicReference<>();
  private final AtomicReference<Destination> acknowledgeTo = new AtomicReference<>();
  private final ConcurrentMap<Message.Priority, MessageConsumer> queueConsumers = new ConcurrentHashMap<>();
  private final int minimumStandardCapacity;


  private JMSRequestProxy(String contextFactoryName, String contextURL, String connectionFactoryName,
                          String username, String password, Map<String, String> connectionProperties,
                          String queueName, String topicName, int priority,
                          int maxConcurrentCallsStandard, int maxConcurrentCallsBulk, int maxConcurrentCallsExpedite,
                          int maxMessageSize, RequestSink requestSink, long shutdownTimeout,
                          Collection<MessageSerializer> serializers, int minimumStandardCapacity) {
    super(contextFactoryName, contextURL, connectionFactoryName,
        username, password, connectionProperties,
        queueName, topicName,
        priority, maxMessageSize);

    Objects.requireNonNull(topicName, "topicName not set");
    Objects.requireNonNull(queueName, "queueName not set");
    if (maxConcurrentCallsStandard < 1) throw new IllegalArgumentException("maxConcurrentCallsStandard must be at least 1");
    if (maxConcurrentCallsBulk < 1) throw new IllegalArgumentException("maxConcurrentCallsBulk must be at least 1");
    if (maxConcurrentCallsExpedite < 1) throw new IllegalArgumentException("maxConcurrentCallsExpedite must be at least 1");

    //create common executor for all priority levels, with capacity to cover all priorities
    int totalThreads = maxConcurrentCallsBulk + maxConcurrentCallsExpedite + maxConcurrentCallsStandard;
    //use a synchronous queue, which does direct handoff to executing threads
    BlockingQueue<Runnable> workQueue = new SynchronousQueue<>();
    this.executor = new ThreadPoolExecutor(
        totalThreads / 2, totalThreads, 10, TimeUnit.SECONDS, workQueue,
        new ThreadFactoryBuilder().setNamePrefix("JMSRequestProxy").build()
    );
    //create semaphores to guard the capacity for each level
    standardSemaphore = new Semaphore(maxConcurrentCallsStandard);
    bulkSemaphore = new Semaphore(maxConcurrentCallsBulk);
    expediteSemaphore = new Semaphore(maxConcurrentCallsExpedite);

    this.minimumStandardCapacity = minimumStandardCapacity;
    this.shutdownTimeout = shutdownTimeout;
    this.serializers = configureSerializersWithDefault(serializers);
    this.requestSink = assertNotNull(requestSink, "requestSink not set");
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    MetricsGroup m = new MetricsGroup();
    // Add all server metrics.
    m.addSubMetrics("server", metrics.metrics());
    m.addSubMetrics("requests", new MetricsData()
        .addData("runningRequests", calls.size()) // Approximation as finished contexts are counted until they are cleaned up.
        .addData("standardRequests.pending", standardSemaphore.getQueueLength())
        .addData("standardRequests.available", standardSemaphore.availablePermits())
        .addData("standardRequests.count", ifNotNull(requestCounters.get(Message.Priority.standard), LongAdder::longValue, 0L))
        .addData("standardRequests.reject", ifNotNull(rejectCounters.get(Message.Priority.standard), LongAdder::longValue, 0L))
        .addData("bulkRequests.pending", bulkSemaphore.getQueueLength())
        .addData("bulkRequests.available", bulkSemaphore.availablePermits())
        .addData("bulkRequests.count", ifNotNull(requestCounters.get(Message.Priority.bulk), LongAdder::longValue, 0L))
        .addData("bulkRequests.reject", ifNotNull(rejectCounters.get(Message.Priority.bulk), LongAdder::longValue, 0L))
        .addData("expediteRequests.pending", expediteSemaphore.getQueueLength())
        .addData("expediteRequests.available", expediteSemaphore.availablePermits())
        .addData("expediteRequests.count", ifNotNull(requestCounters.get(Message.Priority.expedite), LongAdder::longValue, 0L))
        .addData("expediteRequests.reject", ifNotNull(rejectCounters.get(Message.Priority.expedite), LongAdder::longValue, 0L))
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
      stopExecutor();
      throw new IllegalStateException(e);
    }
  }


  @Override
  public void stopComponent() {
    try {
      //stop accepting messages
      stopConsumer(Message.Priority.standard);
      stopConsumer(Message.Priority.bulk);
      stopConsumer(Message.Priority.expedite);
      //stop accepting requests
      closed.set(true);
      stopExecutor();
    } catch (Exception e) {
      LOGGER.warning(e, "Error stopping request proxy");
    }
    //now do cleanup of resources
    closeAllResources();
  }


  private void stopConsumer(Message.Priority priority) {
    ifNotNull(
        queueConsumers.get(priority),
        c -> tryTo(() -> c.setMessageListener(null), e -> LOGGER.warning(e, "Error removing message listener"))
    );
  }

  private void stopExecutor() {
    if (executor == null) return;
    //stop executor
    executor.shutdown();
    //wait for ongoing requests to finish
    //noinspection ResultOfMethodCallIgnored
    tryTo(
        () -> executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS),
        e -> LOGGER.warning("Error waiting for executor termination")
    );
  }

  private void connect() throws NamingException, JMSException {
    synchronized (this) {
      //create common reply producer
      MessageProducer p = getSession().createProducer(null);
      p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      replyProducer.set(p);

      //create consumers per priority level
      createConsumer(Message.Priority.standard, "JMSPriority >= 4 AND JMSPriority <= 6");
      createConsumer(Message.Priority.bulk, "JMSPriority < 4");
      createConsumer(Message.Priority.expedite, "JMSPriority > 6");

      //set up server acknowledgement queue, to receive acknowledgement from client
      TemporaryQueue responseAcknowledgementQueue = getSession().createTemporaryQueue();
      MessageConsumer responseAcknowledgementConsumer = getSession().createConsumer(responseAcknowledgementQueue);
      responseAcknowledgementConsumer.setMessageListener(this::handleResponseAcknowledgement);
      allConsumers.add(responseAcknowledgementConsumer);
      acknowledgeTo.set(responseAcknowledgementQueue);

      //set up topic to receive broadcasts
      MessageConsumer tc = getSession().createConsumer(getTopic());
      tc.setMessageListener(this::processTopicMessage);
      allConsumers.add(tc);

      //notify any connection listeners that setup is done
      set(connectionListeners).forEach(l -> l.connected(this));
      LOGGER.info("Connected");
      metrics.reconnected();
    }
  }

  private void handleResponseAcknowledgement(javax.jms.Message message) {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< handleResponseAcknowledgement");
    }
    try {
      if (!isCompatible(message)) {
        LOGGER.warning("Ignoring acknowledgement message of incompatible version: " + message);
        metrics.incompatibleMessage();
        return;
      }
      String messageType = message.getStringProperty(PROPERTY_MESSAGE_TYPE);
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< process acknowledgement [callID=%s type=%s]", message.getJMSCorrelationID(), messageType);
      }
      if (MESSAGE_TYPE_CLIENT_RESPONSE_ACKNOWLEDGEMENT.equals(messageType)) {
        ServerContext serverContext = calls.get(message.getJMSCorrelationID());
        if (serverContext == null) {
          LOGGER.info("Received acknowledgement for unknown callID : " + message.getJMSCorrelationID());
          return;
        }
        serverContext.acknowledgeResponse();
      } else {
        LOGGER.warning("Received unexpected message on acknowledgement queue: %s", messageType);
      }

    } catch (Exception e) {
      metrics.error();
      LOGGER.warning(e, "Error handling message");
    }
  }

  private void createConsumer(Message.Priority priority, String messageSelector) throws JMSException, NamingException {
    //noinspection resource
    MessageConsumer consumer = createConsumerSession().createConsumer(getQueue(), messageSelector);
    consumer.setMessageListener(msg -> {
      try (LocalLoggingContext ctx = LocalLoggingContext.create().using(LOG_KEY_PRIORITY, priority.name())) {
        processQueueMessage(msg, priority);
      }
    });
    queueConsumers.put(priority, consumer);
    allConsumers.add(consumer);
  }

  private Session createConsumerSession() throws NamingException, JMSException {
    Session session = createSession();
    consumerSessions.add(session);
    return session;
  }

  private synchronized void closeAllResources() {
    try {
      // try to nicely shut down all resources
      executeAndReset(replyProducer, MessageProducer::close, "Error closing reply producer");
      for (MessageConsumer consumer : allConsumers) {
        closeConsumer(consumer);
      }
      executeAndReset(session, Session::close, "Error closing session");
      for (Session s : consumerSessions) {
        tryTo(s::close);
      }
      allConsumers.clear();
      consumerSessions.clear();
      executeAndReset(connection, Connection::close, "Error closing connection");
    } finally {
      resetState();
    }
  }

  private synchronized void resetState() {
    queueConsumers.clear();
    allConsumers.clear();
    replyProducer.set(null);
    session.set(null);
    consumerSessions.clear();
    queue.set(null);
    topic.set(null);
    connection.set(null);
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
  private void processQueueMessage(javax.jms.Message message, Message.Priority priority) {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< processQueueMessage [priority=%s]", priority);
    }
    metrics.request();
    checkCleanRequests();
    try {
      if (!isCompatible(message)) {
        LOGGER.warning("Ignoring queue request of incompatible version: " + message);
        metrics.incompatibleMessage();
        return;
      }

      //if message is past timeout already when being consumed, do not bother scheduling it
      long timeout = message.getLongProperty(PROPERTY_REQ_TIMEOUT);
      long maxWait = timeout - clock.millis();
      if (maxWait <= 0) {
        LOGGER.warning("Ignoring queued %s request: timed out", priority);
        metrics.requestTimeout();
        return;
      }

      String messageType = message.getStringProperty(PROPERTY_MESSAGE_TYPE);
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< receive queueMessage [callID=%s type=%s priority=%s]", message.getJMSCorrelationID(), messageType, priority);
      }

      try (TimerContext ignored = TimerContext.timerMillis(awaitPermitTime::add)) {
        //schedule message for processing
        acquireSemaphoreAndSchedule(message, priority, messageType, timeout, maxWait);
      }
    } catch (Exception e) {
      metrics.error();
      LOGGER.warning(e, "Error handling message");
    }
  }

  /**
   * Try to acquire a semaphore, and if successful, schedule for execution
   *
   * @param message     the message to schedule
   * @param priority    the priority of the message
   * @param messageType the type of the message
   * @param timeout     the timestamp when this message is timed out
   * @param maxWait     the max ms to wait when scheduling this message
   */
  private void acquireSemaphoreAndSchedule(javax.jms.Message message, Message.Priority priority, String messageType, long timeout, long maxWait) throws JMSException {
    try {
      //determine which semaphore to use
      Semaphore semaphore = selectSemaphore(priority);
      boolean acquired = semaphore.tryAcquire(maxWait, TimeUnit.MILLISECONDS);
      if (acquired) {
        if (LOGGER.isDebug()) {
          LOGGER.debug("<< acquired queueMessage [callID=%s type=%s priority=%s]", message.getJMSCorrelationID(), messageType, priority);
        }
        //schedule for execution
        //NOTE: semaphore is acquired, so must be released!
        scheduleForProcessing(semaphore, message, priority, messageType, timeout);
      } else {
        //Semaphore is NOT acquired, so no release
        rejectCounters.computeIfAbsent(priority, pri -> new LongAdder()).increment();
        metrics.requestTimeout();
        LOGGER.warning("Timed out waiting for %s semaphore", priority);
      }
    } catch (InterruptedException e) {
      //Semaphore is NOT acquired, so no release
      LOGGER.error(e, "Interrupted while waiting for semaphore");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Schedule message for processing
   *
   * @param message     the message to schedule
   * @param priority    the priority of the message
   * @param messageType the type of the message
   * @param timeout     the timestamp when this message is timed out
   */
  private void scheduleForProcessing(Semaphore semaphore, javax.jms.Message message, Message.Priority priority, String messageType, long timeout) {
    try {
      //schedule for execution
      requestCounters.computeIfAbsent(priority, pri -> new LongAdder()).increment();
      //NOTE: semaphore is acquired, so must be released by executor thread
      executor.submit(() -> {
        try (LocalLoggingContext ignored = LocalLoggingContext.create().using(LOG_KEY_PRIORITY, priority.name())) {
          doProcessQueueMessage(message, messageType, timeout, semaphore, priority);
        }
      });
    } catch (RejectedExecutionException e) {
      //IMPORTANT: remember to release the semaphore we got a permit from
      semaphore.release();
      LOGGER.warning("Submission of processing %s request failed: timed out", priority);
      metrics.requestTimeout();
      rejectCounters.computeIfAbsent(priority, pri -> new LongAdder()).increment();
    }
  }

  private Semaphore selectSemaphore(Message.Priority priority) {
    switch (priority) {
      case bulk:
        //try for available capacity in bulk first
        if (bulkSemaphore.availablePermits() > 1) return bulkSemaphore;
        //try for available excess capacity (above minimumStandardCapacity) in standard, to utilize any free capacity
        if (standardSemaphore.availablePermits() > minimumStandardCapacity) return standardSemaphore;
        //fallback to waiting for the bulk semaphore
        return bulkSemaphore;
      case standard:
        //try for available capacity in bulk first, as standard traffic should use any available capacity
        if (bulkSemaphore.availablePermits() > 1) return bulkSemaphore;
        return standardSemaphore;
      case expedite:
        //expedite traffic should use any available capacity
        if (expediteSemaphore.availablePermits() > 1) return expediteSemaphore;
        if (bulkSemaphore.availablePermits() > 1) return bulkSemaphore;
        if (standardSemaphore.availablePermits() > 1) return standardSemaphore;
        return expediteSemaphore;
      default:
        return standardSemaphore;
    }
  }

  /**
   * Actually process the message
   *
   * @param message     message to process
   * @param messageType message type
   * @param timeout     the timestamp when this message is timed out
   * @param semaphore   the semaphore we got a permit from, must be released when done processing
   * @param priority    the priority of the incoming message
   */
  private void doProcessQueueMessage(javax.jms.Message message, String messageType, long timeout, Semaphore semaphore, Message.Priority priority) {
    //if message has timed out while waiting for scheduling, do not bother executing it
    if (clock.millis() > timeout) {
      //IMPORTANT: remember to release the semaphore we got a permit from
      semaphore.release();
      metrics.requestTimeout();
      LOGGER.warning("Ignoring process request: timed out");
      return;
    }
    try {
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< processing queueMessage [callID=%s type=%s priority=%s]", message.getJMSCorrelationID(), messageType, priority);
      }
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
      //IMPORTANT: remember to release the semaphore we got a permit from
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
    ServerResponseContext ctx = setupServerContext(callID,
        responseDestination,
        timeout,
        getSegmentWindowSize(message),
        getProtocolVersion(message),
        serializer);
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
    setupChannel(callID,
        responseDestination,
        timeout,
        getSegmentWindowSize(message),
        getProtocolVersion(message),
        serializer);
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

  private void handleChannelUploadCompleted(String callID,
                                            byte[] data,
                                            Destination replyTo,
                                            long timeout,
                                            int segmentWindowSize,
                                            ProtocolVersion protocolVersion,
                                            MessageSerializer serializer
  ) throws IOException, JMSException, NamingException {
    // create a response context to handle response messages
    ServerResponseContext r = ServerResponseContext.builder()
        .setCallID(callID)
        .setSession(getSession())
        .setReplyProducer(replyProducer.get())
        .setReplyTo(replyTo)
        .setAcknowledgementTo(acknowledgeTo.get())
        .setTimeout(timeout)
        .setProtocolVersion(protocolVersion)
        .setMaxMessageSize(getMaxMessageSize())
        .setSegmentWindowSize(segmentWindowSize)
        .setMetrics(metrics)
        .setSerializer(serializer)
        .setRequestSink(requestSink)
        .build();
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
    if (clock.millis() - lastCleanupTimestamp.get() < 10000) return;
    lastCleanupTimestamp.set(clock.millis());
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
   * @param timeout how long this responsesink will forward response messages
   * @return a responsesink fulfilling this API
   */
  private ServerResponseContext setupServerContext(
      final String callID,
      Destination replyTo,
      long timeout,
      int segmentWindowSize,
      ProtocolVersion protocolVersion,
      MessageSerializer serializer
  ) throws JMSException, NamingException {
    ServerContext ctx = calls.get(callID);
    if (ctx != null) return (ServerResponseContext) ctx;
    //create new response context
    ServerResponseContext context = ServerResponseContext.builder()
        .setCallID(callID)
        .setSession(getSession())
        .setReplyProducer(replyProducer.get())
        .setReplyTo(replyTo)
        .setAcknowledgementTo(acknowledgeTo.get())
        .setTimeout(timeout)
        .setProtocolVersion(protocolVersion)
        .setMaxMessageSize(getMaxMessageSize())
        .setSegmentWindowSize(segmentWindowSize)
        .setMetrics(metrics)
        .setSerializer(serializer)
        .setRequestSink(requestSink)
        .build();
    // register this responsesink
    calls.put(callID, context);
    // and return it
    return context;
  }

  private void setupChannel(String callID,
                            Destination replyTo,
                            long timeout,
                            int segmentWindowSize,
                            ProtocolVersion protocolVersion,
                            MessageSerializer serializer) throws NamingException, JMSException {
    metrics.fragmentedUploadRequested();
    ServerContext ctx = calls.get(callID);
    if (ctx != null) return;
    //create new upload context
    ServerChannelUploadContext context = new ServerChannelUploadContext(callID, getSession(), replyTo, timeout, protocolVersion, segmentWindowSize, metrics, serializer);
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
    private int maxConcurrentCallsStandard = DEFAULT_MAX_CONCURRENT_CALLS_STANDARD;
    private int maxConcurrentCallsBulk = DEFAULT_MAX_CONCURRENT_CALLS_BULK;
    private int maxConcurrentCallsExpedite = DEFAULT_MAX_CONCURRENT_CALLS_EXPEDITE;
    private int minimumStandardCapacity = DEFAULT_MINIMUM_STANDARD_CAPACITY;
    private int shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;
    private List<MessageSerializer> serializers = ListUtils.list();

    private Builder() {
    }

    //fields

    public JMSRequestProxy build() {
      return new JMSRequestProxy(
          contextFactoryName, contextURL, connectionFactoryName, username, password,
          connectionProperties, queueName, topicName, priority,
          maxConcurrentCallsStandard, maxConcurrentCallsBulk, maxConcurrentCallsExpedite,
          maxMessageSize, requestSink, shutdownTimeout, serializers, minimumStandardCapacity);
    }

    //setters


    /**
     * @deprecated Use {@link #setMaxConcurrentCallsStandard(int)}
     */
    @Deprecated
    public Builder setMaxConcurrentCalls(int maxConcurrentCallsStandard) {
      this.maxConcurrentCallsStandard = maxConcurrentCallsStandard;
      return this;
    }

    public Builder setMaxConcurrentCallsStandard(int maxConcurrentCallsStandard) {
      this.maxConcurrentCallsStandard = maxConcurrentCallsStandard;
      return this;
    }

    public Builder setMaxConcurrentCallsBulk(int maxConcurrentCallsBulk) {
      this.maxConcurrentCallsBulk = maxConcurrentCallsBulk;
      return this;
    }

    public Builder setMaxConcurrentCallsExpedite(int maxConcurrentCallsExpedite) {
      this.maxConcurrentCallsExpedite = maxConcurrentCallsExpedite;
      return this;
    }

    public Builder setMinimumStandardCapacity(int minimumStandardCapacity) {
      this.minimumStandardCapacity = minimumStandardCapacity;
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
