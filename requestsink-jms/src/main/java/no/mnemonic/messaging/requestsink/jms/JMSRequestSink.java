package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.utilities.ObjectUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.*;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.time.Clock;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static no.mnemonic.commons.utilities.collections.SetUtils.set;

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
 * upon single server restart or network reconfiguration. To detect this situation, the JMSRequestSink will
 * detect if there are no received replies on the response channel inside of <code>staleResponseQueueTimeout</code> milliseconds, in which
 * case it will create a new response queue. To disable this mechanism, set <code>staleResponseQueueTimeout</code> to 0.
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
public class JMSRequestSink extends JMSBase implements RequestSink, RequestListener, MessageListener, MetricAspect {

  private static final Logger LOGGER = Logging.getLogger(JMSRequestSink.class);
  private static final int DEFAULT_STALE_RESPONSEQUEUE_TIMEOUT = 10000;
  private static final Clock clock = Clock.systemUTC();

  private final ProtocolVersion protocolVersion;

  // variables

  private final ConcurrentHashMap<String, ClientRequestState> requests = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ClientResponseListener> responseListeners = new ConcurrentHashMap<>();
  private final ExecutorService executor;
  private final AtomicLong lastRequestSent = new AtomicLong();
  private final AtomicLong lastResponseReceived = new AtomicLong();

  private MessageProducer producer;
  private TemporaryQueue responseQueue;
  private MessageConsumer responseConsumer;

  private final ClientMetrics metrics = new ClientMetrics();
  private final long staleResponseQueueTimeout;

  private JMSRequestSink(String contextFactoryName, String contextURL, String connectionFactoryName,
                         String username, String password, Map<String, String> connectionProperties,
                         String destinationName,
                         int priority, int maxMessageSize, ProtocolVersion protocolVersion, long staleResponseQueueTimeout) {
    super(contextFactoryName, contextURL, connectionFactoryName, username, password, connectionProperties, destinationName,
            priority, maxMessageSize);
    this.staleResponseQueueTimeout = staleResponseQueueTimeout;
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
      lastResponseReceived.set(clock.millis());
      ClientResponseListener responseListener = responseListeners.get(message.getJMSCorrelationID());
      if (responseListener == null) {
        metrics.unknownCallIDMessage();
        LOGGER.warning("No response handler for callID: %s (type=%s)",
                message.getJMSCorrelationID(),
                message.getStringProperty(PROPERTY_MESSAGE_TYPE));
        return;
      }
      responseListener.handleResponse(message);
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

    //periodic verification of response queue to detect stale response queue
    checkForStaleResponseQueue();

    //if no context given by user, create a null context
    RequestContext ctx = ObjectUtils.ifNull(signalContext, NullRequestContext::new);

    //register for client-side notifications
    ctx.addListener(this);

    checkForFragmentationAndSignal(msg, ctx, maxWait);

    //clean response state on every request
    executor.submit(this::cleanResponseState);

    return signalContext;
  }

  @Override
  public void close(String callID) {
    //close the specified request
    requests.remove(callID);
    responseListeners.remove(callID);
  }

  @Override
  public void onException(JMSException e) {
    super.onException(e);

    closeAndRecreateTemporaryResources(e);
  }

  @Override
  public void startComponent() {
    setupProducerAndConsumer();
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

  private void closeAndRecreateTemporaryResources(Exception e) {
    if (e == null) throw new IllegalArgumentException("Exception should never be null");

    //only allow one thread at a time into this block
    synchronized (this) {
      //first thread into this block will reset the lastRequestSent
      //causing all other pending threads to skip it
      if (lastRequestSent.get() > 0) {
        LOGGER.warning("Invalidating temporary response queue");

        JMSUtils.closeProducer(producer);
        JMSUtils.closeConsumer(responseConsumer);
        JMSUtils.deleteTemporaryQueue(responseQueue);

        //on exception, notify all ongoing requests of the error
        ListUtils.list(requests.values()).forEach(ctx -> LambdaUtils.tryTo(
                () -> ctx.getRequestContext().notifyError(e),
                ex -> LOGGER.warning(ex, "Error calling requestContext.notifyErrorToClient")
        ));
        //and close all requests
        set(requests.keySet()).forEach(this::close);

        setupProducerAndConsumer();

        //reset stale queue detection counters
        lastRequestSent.set(0);
        lastResponseReceived.set(0);
      }
    }
  }

  private void checkForStaleResponseQueue() {
    if (staleResponseQueueTimeout > 0 && lastRequestSent.get() > 0) {
      long diff = lastRequestSent.get() - lastResponseReceived.get();
      if (diff > staleResponseQueueTimeout) {
        closeAndRecreateTemporaryResources(new IllegalStateException("Stale response queue"));
      }
    }
  }

  private void setupProducerAndConsumer() {
    try {
      producer = getSession().createProducer(getDestination());
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      responseQueue = getSession().createTemporaryQueue();
      responseConsumer = getSession().createConsumer(responseQueue);
      responseConsumer.setMessageListener(this);
    } catch (JMSException | NamingException e1) {
      LOGGER.error(e1, "Error reestablishing producers and consumers");
    }
  }

  private void cleanResponseState() {
    for (Map.Entry<String, ClientRequestState> e : set(requests.entrySet())) {
      ClientRequestState requestHandler = e.getValue();
      if (requestHandler.getRequestContext().isClosed()) {
        close(e.getKey());
      }
    }
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
      //add handler for this request
      ClientRequestState handler = new ClientRequestState(ctx, Thread.currentThread().getContextClassLoader());
      //setup response listener for this request
      ClientResponseListener responseListener = new ClientResponseListener(msg.getCallID(), getSession(), handler, metrics);
      //register handler
      requests.put(msg.getCallID(), handler);
      responseListeners.put(msg.getCallID(), responseListener);
      //send signal message
      sendMessage(messageBytes, msg.getCallID(), messageType, maxWait, responseQueue);
      lastRequestSent.set(clock.millis());
      metrics.request();
    } catch (IOException | JMSException | NamingException e) {
      LOGGER.warning(e, "Error in checkForFragmentationAndSignal");
      throw new IllegalStateException(e);
    }
  }

  private void sendMessage(byte[] messageBytes, String callID, String messageType, long lifeTime, Destination replyTo) {
    try {
      javax.jms.Message m = JMSUtils.createByteMessage(getSession(), messageBytes, protocolVersion);
      long timeout = System.currentTimeMillis() + lifeTime;
      m.setJMSReplyTo(replyTo);
      m.setJMSCorrelationID(callID);
      m.setStringProperty(PROPERTY_MESSAGE_TYPE, messageType);
      m.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, timeout);
      producer.send(m, DeliveryMode.NON_PERSISTENT, getPriority(), lifeTime);
      if (LOGGER.isDebug()) {
        LOGGER.debug(">> sendMessage [destination=%s callID=%s messageType=%s replyTo=%s timeout=%s]", getDestination(), callID, messageType, replyTo, new Date(timeout));
      }
    } catch (Exception e) {
      LOGGER.warning(e, "Error in sendMessage");
      throw new MessagingException(e);
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
    private long staleResponseQueueTimeout = DEFAULT_STALE_RESPONSEQUEUE_TIMEOUT;

    private Builder() {
    }

    public JMSRequestSink build() {
      return new JMSRequestSink(contextFactoryName, contextURL, connectionFactoryName,
              username, password, connectionProperties, destinationName,
              priority, maxMessageSize, protocolVersion, staleResponseQueueTimeout);
    }

    //setters

    public Builder setProtocolVersion(ProtocolVersion protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
    }

    public Builder setStaleResponseQueueTimeout(long staleResponseQueueTimeout) {
      this.staleResponseQueueTimeout = staleResponseQueueTimeout;
      return this;
    }
  }

}
