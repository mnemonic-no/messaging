package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsGroup;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.MessagingException;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestListener;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.messaging.requestsink.jms.context.ChannelUploadMessageContext;
import no.mnemonic.messaging.requestsink.jms.context.ClientRequestContext;
import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import no.mnemonic.messaging.requestsink.jms.serializer.MessageSerializer;
import no.mnemonic.messaging.requestsink.jms.util.ClientMetrics;
import no.mnemonic.messaging.requestsink.jms.util.ThreadFactoryBuilder;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.*;

/**
 * JMSRequestSink is a JMS implementation of the RequestSink using JMS,
 * communicating over JMS to a corresponding JMSRequestProxy.
 * <p>
 * <pre>
 *   ------------        ------------------                     -------------------     ---------------
 *   | Client   |  --   | JMSRequestSink | ------- JMS -------  | JMSRequestProxy |  -- | RequestSink |
 *   ------------        ------------------                     -------------------     ---------------
 * </pre>
 * <p>
 * The messages must implement the {@link Message} interface, and contain a <code>callID</code> and a <code>timestamp</code>.
 * Other than this, the JMSRequestSink does not care about the format of the sent messages, but will deliver them to the
 * RequestSink on the other side, where they must be handled.
 * <p>
 * The messaging protocol between a JMSRequestSink and JMSRequestProxy is private between these entities, and should be
 * concidered transparent.
 * <p>
 * The implementation will serialize messages to bytes. If upstream messages are above the configured maxMessageSize,
 * the JMSRequestSink will create an upload channel to establish a temporary queue for uploading, and will fragment the upload
 * message into multiple messages on the upload channel. This avoids very large JMS messages for upload.
 * <p>
 * For download, the server RequestSink can choose to send multiple replies on the same reply channel, and can use this
 * to stream the results back to the client.
 * <p>
 * The JMSRequestSink uses a single multiplexed temporary response to receive all responses for signalled requests.
 * This reduces the load on the JMS server infrastructure, and gives a more stable system in clustered/networked JMS environments,
 * since it reduces the need for signalling and cross-network state updates. However, the response queue may become stale, i.e.
 * upon single server restart or network reconfiguration. Clients should notify the requestsink when unexpected timeout occurs, to
 * request that the temporary queue be recreated.
 * <p>
 * The protocol between the sink and the proxy is versioned. The proxy must support the version used by the sink, or the
 * request will fail. The sink can specify a lesser protocol version, allowing a rolling upgrade by upgrading the code first, but keep
 * using previous version until all components are supporting the new protocol version.
 * <p>
 * The default protocol version is V2, so clients must explicitly enable higher protocol versions.
 * <p>
 * Protocol change history:
 * V1 - Initial version, supports requests with multiple replies (streaming result) and upload channel for fragmented request (for large request messages)
 * V2 - Added support for fragmented response (for large single-object response messages)
 * V3 - Added support for custom message serializers. The client serializer must be supported on the server side, but the server can support multiple serializers.
 */
public class JMSRequestSink extends AbstractJMSRequestBase implements RequestSink, MessageListener, MetricAspect {

  private static final Logger LOGGER = Logging.getLogger(JMSRequestSink.class);
  private static final byte[] CHANNEL_CLOSED_REQUEST_MSG = "channel close request".getBytes();
  private static final byte[] CHANNEL_UPLOAD_REQUEST_MSG = "channel upload request".getBytes();
  private static final int ABORT_MSG_LIFETIME_MS = 1000;

  private final ProtocolVersion protocolVersion;

  // variables

  private final ConcurrentHashMap<String, ClientRequestContext> requestHandlers = new ConcurrentHashMap<>();
  private final ExecutorService executor;

  private final AtomicReference<MessageProducer> queueProducer = new AtomicReference<>();
  private final AtomicReference<MessageProducer> topicProducer = new AtomicReference<>();
  private final AtomicReference<ResponseQueueState> currentResponseQueue = new AtomicReference<>();
  private final Set<ResponseQueueState> invalidatedResponseQueues = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final AtomicBoolean cleanupRunning = new AtomicBoolean();
  private final MessageSerializer serializer;

  private final ClientMetrics metrics = new ClientMetrics();

  private boolean cleanupInSeparateThread = true;

  private JMSRequestSink(String contextFactoryName, String contextURL, String connectionFactoryName,
                         String username, String password, Map<String, String> connectionProperties,
                         String queueName, String topicName,
                         int priority, int maxMessageSize, ProtocolVersion protocolVersion, MessageSerializer serializer) {
    super(contextFactoryName, contextURL, connectionFactoryName, username, password, connectionProperties,
        queueName, topicName, priority, maxMessageSize);
    //do not use custom serializer unless version V3 is enabled
    this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNamePrefix("JMSRequestSink").build());
    this.protocolVersion = assertNotNull(protocolVersion, "protocolVersion not set");
    if (serializer == null || !protocolVersion.atLeast(ProtocolVersion.V3))
      serializer = new DefaultJavaMessageSerializer();
    this.serializer = serializer;
  }

  // **************** interface methods **************************

  @Override
  public Metrics getMetrics() throws MetricException {
    MetricsGroup m = new MetricsGroup();
    // Add all client metrics.
    m.addSubMetrics("client", metrics.metrics());
    // Add serializer metrics.
    m.addSubMetrics(serializer.serializerID(), serializer.getMetrics());

    return m;
  }

  @Override
  public void onMessage(javax.jms.Message message) {
    try {
      if (!isCompatible(message)) {
        LOGGER.warning("Ignoring message of incompatible version");
        metrics.incompatibleMessage();
        return;
      }
      if (message.getJMSCorrelationID() == null) {
        LOGGER.warning("Message received without callID");
        metrics.incompatibleMessage();
        return;
      }
      ClientRequestContext handler = requestHandlers.get(message.getJMSCorrelationID());
      if (handler == null) {
        //do not notify/count close message as missing handler, as single-value replies often lead to client-initiated stream close
        if (MESSAGE_TYPE_STREAM_CLOSED.equals(message.getStringProperty(PROPERTY_MESSAGE_TYPE))) {
          LOGGER.debug("No request handler for callID: %s (type=%s)",
              message.getJMSCorrelationID(),
              MESSAGE_TYPE_STREAM_CLOSED);
          return;
        }
        metrics.unknownCallIDMessage();
        LOGGER.warning("No request handler for callID: %s (type=%s)",
            message.getJMSCorrelationID(),
            message.getStringProperty(PROPERTY_MESSAGE_TYPE));
        return;
      }
      handler.handleResponse(message);
    } catch (Exception e) {
      metrics.error();
      LOGGER.error(e, "Error receiving message");
    }
  }

  @Override
  public <T extends RequestContext> T signal(Message msg, final T signalContext, long maxWait) {
    if (isClosed()) throw new IllegalStateException(ERROR_CLOSED);
    //make sure message has callID set
    if (msg.getCallID() == null) {
      throw new IllegalArgumentException("Cannot accept message with no callID");
    }
    //if no context given by user, create a null context
    RequestContext ctx = ifNull(signalContext, NullRequestContext::new);
    //do signal
    checkForFragmentationAndSignal(msg, ctx, maxWait);
    //schedule clean state on every request
    scheduleStateCleanup();
    return signalContext;
  }

  @Override
  public void abort(String callID) {
    MessageProducer topic = getOrCreateTopicProducer();
    if (topic == null) {
      LOGGER.warning("abort() is not supported until a broadcast topic has been configured.");
      return;
    }
    sendMessage(topic, null, CHANNEL_CLOSED_REQUEST_MSG, callID,
        MESSAGE_TYPE_STREAM_CLOSED, getPriority(), ABORT_MSG_LIFETIME_MS);
  }

  @Override
  public void onException(JMSException e) {
    //replace response queue on received exception
    replaceResponseQueue();
  }

  @Override
  public void startComponent() {
    try {
      //prepare producer
      getOrCreateQueueProducer();
      //initialize response queue
      replaceResponseQueue();
    } catch (Exception e) {
      executor.shutdown();
      throw new IllegalStateException("Error setting up connection", e);
    }
  }

  @Override
  public void stopComponent() {
    //stop accepting requests
    closed.set(true);
    //shutdown executor and wait for it to finish current requests
    executor.shutdown();
    LambdaUtils.tryTo(
        () -> executor.awaitTermination(10, TimeUnit.SECONDS),
        e -> LOGGER.warning(e, "Error waiting for executor termination")
    );
    //close all resources
    closeAllResources();
  }

  // ****************** private methods ************************

  private MessageProducer getOrCreateQueueProducer() {
    return queueProducer.updateAndGet(prod -> {
      if (prod != null) return prod;
      try {
        prod = getSession().createProducer(getQueue());
        prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return prod;
      } catch (JMSException | NamingException e) {
        throw new IllegalStateException("Error setting up queue producer", e);
      }
    });
  }

  private MessageProducer getOrCreateTopicProducer() {
    return topicProducer.updateAndGet(p -> {
      if (p != null) return p;
      try {
        //if topic is not supported, return null
        Optional<Topic> optionalTopic = getTopic();
        if (!optionalTopic.isPresent()) return null;
        MessageProducer producer = getSession().createProducer(optionalTopic.get());
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return producer;
      } catch (JMSException | NamingException e) {
        throw new IllegalStateException("Error setting up topic producer", e);
      }
    });
  }

  private void scheduleStateCleanup() {
    //only schedule cleanup if other cleanup is not already running
    if (!cleanupRunning.compareAndSet(false, true)) return;

    if (cleanupInSeparateThread) {
      executor.submit(this::cleanState);
    } else {
      cleanState();
    }
  }

  private ResponseQueueState replaceResponseQueue() {
    try {
      //create new temporary queue and set a message consumer on it
      TemporaryQueue queue = getSession().createTemporaryQueue();
      MessageConsumer consumer = getSession().createConsumer(queue);
      consumer.setMessageListener(this);
      ResponseQueueState newState = new ResponseQueueState(queue, consumer);
      LOGGER.info("Created new response queue %s", newState.getResponseQueue());

      //add to list of response queues and set as current active responsequeue
      ResponseQueueState oldState = currentResponseQueue.getAndUpdate(s -> newState);

      //mark old responsequeue as invalidated (if set)
      ifNotNullDo(oldState, s -> {
        LOGGER.warning("Invalidating response queue %s", s.getResponseQueue());
        metrics.invalidatedResponseQueue();
        invalidatedResponseQueues.add(oldState);
      });

      return newState;
    } catch (JMSException | NamingException e) {
      //if exception is caught when setting up new response queue, we are probably truly disconnected, so close ALL resources and let next request reconnect
      closeAllResources();
      throw new IllegalStateException(e);
    }
  }

  private void cleanState() {
    try {
      //cleanup pending calls
      for (ClientRequestContext handler : list(requestHandlers.values())) {
        if (handler.isClosed()) {
          cleanupRequest(handler);
        }
      }
      //cleanup old responsequeues
      for (ResponseQueueState s : list(invalidatedResponseQueues)) {
        if (s.isIdle()) {
          s.close();
          invalidatedResponseQueues.remove(s);
        }
      }
    } finally {
      cleanupRunning.set(false);
    }
  }

  private void cleanupRequest(ClientRequestContext handler) {
    if (!requestHandlers.containsKey(handler.getCallID())) {
      return;
    }
    if (LOGGER.isDebug()) {
      LOGGER.debug("## cleanup [callID=%s]", handler.getCallID());
    }
    //close the specified request
    requestHandlers.remove(handler.getCallID());
    handler.cleanup();
  }

  private void checkForFragmentationAndSignal(Message msg, RequestContext ctx, long maxWait) {
    try {
      byte[] messageBytes = serializer.serialize(msg);
      int priority = resolveJMSPriority(msg);
      String messageType = JMSRequestProxy.MESSAGE_TYPE_SIGNAL;
      //check if we need to fragment this request message
      if (messageBytes.length > getMaxMessageSize()) {
        //if needing to fragment, replace signal context with a wrapper client upload context and send a channel request
        ctx = new ChannelUploadMessageContext(ctx, new ByteArrayInputStream(messageBytes), msg.getCallID(), getMaxMessageSize(), protocolVersion, metrics);
        messageBytes = CHANNEL_UPLOAD_REQUEST_MSG;
        messageType = JMSRequestProxy.MESSAGE_TYPE_CHANNEL_REQUEST;
        metrics.fragmentedUploadRequested();
      }
      //select response queue to use for this request
      ResponseQueueState currentResponseQueue = getCurrentResponseQueueState();
      //setup handler for this request
      ClientRequestContext handler = new ClientRequestContext(
          msg.getCallID(), getSession(), metrics,
          Thread.currentThread().getContextClassLoader(), ctx,
          () -> currentResponseQueue.endCall(msg.getCallID()), serializer);

      //register handler
      requestHandlers.put(msg.getCallID(), handler);
      //register call in current response queue
      currentResponseQueue.addCall(msg.getCallID());
      //register for client-side notifications
      ctx.addListener(new RequestListener() {
        @Override
        public void close(String callID) {
          cleanupRequest(handler);
        }

        @Override
        public void abort(String callID) {
          JMSRequestSink.this.abort(callID);
          cleanupRequest(handler);
        }

        @Override
        public void timeout() {
          replaceResponseQueue();
        }
      });
      //send signal message
      sendMessage(getOrCreateQueueProducer(), getCurrentResponseQueue(), messageBytes, msg.getCallID(), messageType, priority, maxWait);
      metrics.request();
    } catch (IOException | JMSException | NamingException e) {
      LOGGER.warning(e, "Error in checkForFragmentationAndSignal");
      throw new IllegalStateException(e);
    }
  }

  /**
   * JMS priority is defined as an integer between 1 and 9, where 4 is the default.
   *
   * @param msg the Message to prioritize
   * @return the JMS priority. Bulk will be mapped to 1, expedite to 9.
   */
  private int resolveJMSPriority(Message msg) {
    if (msg.getPriority() == Message.Priority.bulk) return 1;
    if (msg.getPriority() == Message.Priority.expedite) return 9;
    return 4;
  }

  private Destination getCurrentResponseQueue() {
    return getCurrentResponseQueueState().getResponseQueue();
  }

  private ResponseQueueState getCurrentResponseQueueState() {
    return ifNull(currentResponseQueue.get(), this::replaceResponseQueue);
  }

  private void sendMessage(MessageProducer producer, Destination replyTo, byte[] messageBytes, String callID, String messageType, int priority, long lifeTime) {
    if (producer == null) {
      throw new IllegalArgumentException("Producer not set");
    }
    try {
      javax.jms.Message m = createByteMessage(getSession(), messageBytes, protocolVersion, serializer.serializerID());
      long timeout = System.currentTimeMillis() + lifeTime;
      m.setJMSReplyTo(replyTo);
      m.setJMSCorrelationID(callID);
      m.setJMSPriority(priority);
      m.setStringProperty(PROPERTY_MESSAGE_TYPE, messageType);
      m.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, timeout);
      producer.send(m, DeliveryMode.NON_PERSISTENT, getPriority(), lifeTime);
      if (LOGGER.isDebug()) {
        LOGGER.debug(">> sendMessage [callID=%s messageType=%s replyTo=%s timeout=%s]", callID, messageType, replyTo, new Date(timeout));
      }
    } catch (Exception e) {
      LOGGER.warning(e, "Error in sendMessage");
      //if exception is caught when preparing/sending message, we are truly disconnected, so close ALL resources and let next request reconnect
      closeAllResources();
      throw new MessagingException(e);
    }
  }

  private synchronized void closeAllResources() {
    LOGGER.warning("Resetting connection");
    metrics.disconnected();
    try {
      // try to nicely shut down all resources
      executeAndReset(currentResponseQueue, ResponseQueueState::close, "Error closing response queue");
      executeAndReset(queueProducer, MessageProducer::close, "Error closing queue producer");
      executeAndReset(topicProducer, MessageProducer::close, "Error closing topic producer");
      executeAndReset(session, Session::close, "Error closing session");
      executeAndReset(connection, Connection::close, "Error closing connection");
      invalidatedResponseQueues.forEach(ResponseQueueState::close);
    } finally {
      resetState();
    }
  }

  private synchronized void resetState() {
    currentResponseQueue.set(null);
    queueProducer.set(null);
    topicProducer.set(null);
    session.set(null);
    queue.set(null);
    topic.set(null);
  }

  private static class ResponseQueueState {
    private final TemporaryQueue responseQueue;
    private final MessageConsumer responseConsumer;
    private final Set<String> activeCalls = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private ResponseQueueState(TemporaryQueue responseQueue, MessageConsumer responseConsumer) {
      this.responseQueue = responseQueue;
      this.responseConsumer = responseConsumer;
    }

    TemporaryQueue getResponseQueue() {
      return responseQueue;
    }

    void addCall(String callID) {
      activeCalls.add(callID);
    }

    void endCall(String callID) {
      activeCalls.remove(callID);
    }

    boolean isIdle() {
      return activeCalls.isEmpty();
    }

    void close() {
      closeConsumer(responseConsumer);
      deleteTemporaryQueue(responseQueue);
    }
  }

  private static class NullRequestContext implements RequestContext {
    private NullRequestContext() {
    }

    public boolean addResponse(Message msg) {
      return false;
    }

    public boolean keepAlive(long until) {
      return false;
    }

    public void endOfStream() {
      //ignore
    }

    public boolean isClosed() {
      return true;
    }

    public void notifyError(Throwable e) {
      //ignore
    }

    @Override
    public void notifyClose() {
      //ignore
    }

    public void addListener(RequestListener listener) {
      //ignore
    }

    public void removeListener(RequestListener listener) {
      //ignore
    }
  }

  @SuppressWarnings("WeakerAccess")
  public static Builder builder() {
    return new Builder();
  }

  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class Builder extends AbstractJMSRequestBase.BaseBuilder<Builder> {

    //fields
    private ProtocolVersion protocolVersion = ProtocolVersion.V1;
    private MessageSerializer serializer = new DefaultJavaMessageSerializer();

    private Builder() {
    }

    public JMSRequestSink build() {
      return new JMSRequestSink(
          contextFactoryName, contextURL, connectionFactoryName,
          username, password, connectionProperties,
          queueName, topicName,
          priority, maxMessageSize, protocolVersion, serializer);
    }

    //setters

    public Builder setProtocolVersion(ProtocolVersion protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
    }

    public Builder setSerializer(MessageSerializer serializer) {
      this.serializer = serializer;
      return this;
    }
  }

  //allow turning this of for testing
  void setCleanupInSeparateThread(boolean cleanupInSeparateThread) {
    this.cleanupInSeparateThread = cleanupInSeparateThread;
  }
}
