package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.utilities.ObjectUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.*;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;

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
 * The default protocol version is V1, so clients must explicitly enable higher protocol versions.
 * <p>
 * Protocol change history:
 * V1 - Initial version, supports requests with multiple replies (streaming result) and upload channel for fragmented request (for large request messages)
 * V2 - Added support for fragmented response (for large single-object response messages)
 */
public class JMSRequestSink extends JMSBase implements RequestSink, MessageListener, MetricAspect {

  private static final Logger LOGGER = Logging.getLogger(JMSRequestSink.class);

  private final ProtocolVersion protocolVersion;

  // variables

  private final ConcurrentHashMap<String, ClientRequestHandler> requestHandlers = new ConcurrentHashMap<>();
  private final ExecutorService executor;

  private final AtomicReference<MessageProducer> producer = new AtomicReference<>();
  private final AtomicReference<ResponseQueueState> currentResponseQueue = new AtomicReference<>();
  private final Set<ResponseQueueState> invalidatedResponseQueues = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final AtomicBoolean cleanupRunning = new AtomicBoolean();

  private final ClientMetrics metrics = new ClientMetrics();

  private boolean cleanupInSeparateThread = true;

  private JMSRequestSink(String contextFactoryName, String contextURL, String connectionFactoryName,
                         String username, String password, Map<String, String> connectionProperties,
                         String destinationName,
                         int priority, int maxMessageSize, ProtocolVersion protocolVersion) {
    super(contextFactoryName, contextURL, connectionFactoryName, username, password, connectionProperties, destinationName,
            priority, maxMessageSize);
    if (protocolVersion == null) {
      throw new IllegalArgumentException("protocolVersion not set");
    }
    executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNamePrefix("JMSRequestSink").build());
    this.protocolVersion = protocolVersion;
  }

  // **************** interface methods **************************

  @Override
  public Metrics getMetrics() throws MetricException {
    return metrics.metrics();
  }

  @Override
  public void onMessage(javax.jms.Message message) {
    try {
      if (!JMSUtils.isCompatible(message)) {
        LOGGER.warning("Ignoring message of incompatible version");
        metrics.incompatibleMessage();
        return;
      }
      if (message.getJMSCorrelationID() == null) {
        LOGGER.warning("Message received without callID");
        metrics.incompatibleMessage();
        return;
      }
      ClientRequestHandler handler = requestHandlers.get(message.getJMSCorrelationID());
      if (handler == null) {
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
    //make sure message has callID set
    if (msg.getCallID() == null) {
      throw new IllegalArgumentException("Cannot accept message with no callID");
    }
    //if no context given by user, create a null context
    RequestContext ctx = ObjectUtils.ifNull(signalContext, NullRequestContext::new);
    //do signal
    checkForFragmentationAndSignal(msg, ctx, maxWait);
    //schedule clean state on every request
    scheduleStateCleanup();
    return signalContext;
  }

  @Override
  public void onException(JMSException e) {
    super.onException(e);
    //replace response queue on received exception
    replaceResponseQueue();
  }

  @Override
  public void startComponent() {
    try {
      producer.set(getSession().createProducer(getDestination()));
      producer.get().setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    } catch (JMSException | NamingException e) {
      LOGGER.error(e, "Error setting up connection");
    }
    //initialize response queue
    replaceResponseQueue();
  }

  @Override
  public void stopComponent() {
    super.stopComponent();
    executor.shutdown();
    LambdaUtils.tryTo(
            () -> executor.awaitTermination(10, TimeUnit.SECONDS),
            e -> LOGGER.warning(e, "Error waiting for executor termination")
    );
  }

  // ****************** private methods ************************

  private void scheduleStateCleanup() {
    //only schedule cleanup if other cleanup is not already running
    if (!cleanupRunning.compareAndSet(false, true)) return;
    
    if (cleanupInSeparateThread) {
        executor.submit(this::cleanState);
    } else {
      cleanState();
    }
  }

  private void replaceResponseQueue() {
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
    } catch (JMSException | NamingException e) {
      throw new IllegalStateException(e);
    }
  }

  private void cleanState() {
    try {
      //cleanup pending calls
      for (ClientRequestHandler handler : list(requestHandlers.values())) {
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

  private void cleanupRequest(ClientRequestHandler handler) {
    if (!requestHandlers.containsKey(handler.getCallID())) {
      return;
    }
    //close the specified request
    requestHandlers.remove(handler.getCallID());
    handler.cleanup();
  }

  private void checkForFragmentationAndSignal(Message msg, RequestContext ctx, long maxWait) {
    try {
      byte[] messageBytes = JMSUtils.serialize(msg);
      String messageType = JMSRequestProxy.MESSAGE_TYPE_SIGNAL;
      //check if we need to fragment this request message
      if (messageBytes.length > getMaxMessageSize()) {
        //if needing to fragment, replace signal context with a wrapper client upload context and send a channel request
        ctx = new ChannelUploadMessageContext(ctx, new ByteArrayInputStream(messageBytes), msg.getCallID(), getMaxMessageSize(), protocolVersion, metrics);
        messageBytes = "channel upload request".getBytes();
        messageType = JMSRequestProxy.MESSAGE_TYPE_CHANNEL_REQUEST;
        metrics.fragmentedUploadRequested();
      }
      //select response queue to use for this request
      ResponseQueueState currentResponseQueue = getCurrentResponseQueueState();
      //setup handler for this request
      ClientRequestHandler handler = new ClientRequestHandler(
              msg.getCallID(), getSession(), metrics,
              Thread.currentThread().getContextClassLoader(), ctx,
              () -> currentResponseQueue.endCall(msg.getCallID()));

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
        public void timeout() {
          replaceResponseQueue();
        }
      });
      //send signal message
      sendMessage(messageBytes, msg.getCallID(), messageType, maxWait, getCurrentResponseQueue());
      metrics.request();
    } catch (IOException | JMSException | NamingException e) {
      LOGGER.warning(e, "Error in checkForFragmentationAndSignal");
      throw new IllegalStateException(e);
    }
  }

  private Destination getCurrentResponseQueue() {
    return currentResponseQueue.get().getResponseQueue();
  }

  private ResponseQueueState getCurrentResponseQueueState() {
    return currentResponseQueue.get();
  }

  private void sendMessage(byte[] messageBytes, String callID, String messageType, long lifeTime, Destination replyTo) {
    try {
      javax.jms.Message m = JMSUtils.createByteMessage(getSession(), messageBytes, protocolVersion);
      long timeout = System.currentTimeMillis() + lifeTime;
      m.setJMSReplyTo(replyTo);
      m.setJMSCorrelationID(callID);
      m.setStringProperty(PROPERTY_MESSAGE_TYPE, messageType);
      m.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, timeout);
      producer.get().send(m, DeliveryMode.NON_PERSISTENT, getPriority(), lifeTime);
      if (LOGGER.isDebug()) {
        LOGGER.debug(">> sendMessage [destination=%s callID=%s messageType=%s replyTo=%s timeout=%s]", getDestination(), callID, messageType, replyTo, new Date(timeout));
      }
    } catch (Exception e) {
      LOGGER.warning(e, "Error in sendMessage");
      throw new MessagingException(e);
    }
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
      JMSUtils.closeConsumer(responseConsumer);
      JMSUtils.deleteTemporaryQueue(responseQueue);
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
  public static class Builder extends JMSBase.BaseBuilder<Builder> {

    //fields
    private ProtocolVersion protocolVersion = ProtocolVersion.V1;

    private Builder() {
    }

    public JMSRequestSink build() {
      return new JMSRequestSink(contextFactoryName, contextURL, connectionFactoryName,
              username, password, connectionProperties, destinationName,
              priority, maxMessageSize, protocolVersion);
    }

    //setters

    public Builder setProtocolVersion(ProtocolVersion protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
    }

  }

  JMSRequestSink setCleanupInSeparateThread(boolean cleanupInSeparateThread) {
    this.cleanupInSeparateThread = cleanupInSeparateThread;
    return this;
  }
}
