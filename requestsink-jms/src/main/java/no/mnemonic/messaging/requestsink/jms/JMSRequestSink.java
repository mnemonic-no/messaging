package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.ClassLoaderContext;
import no.mnemonic.commons.utilities.ObjectUtils;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.*;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.TemporaryQueue;
import javax.naming.NamingException;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JMSRequestSink is a JMS implementation of the RequestSink using JMS,
 * communicating over JMS to a corresponding JMSRequestProxy.
 *
 * <pre>
 *   ------------        ------------------                          -------------------      ---------------
 *   | Client   |  -->   | JMSRequestSink | <-------> JMS <------->  | JMSRequestProxy |  --> | RequestSink |
 *   ------------        ------------------                          -------------------      ---------------
 * </pre>
 *
 * The messages must implement the {@link Message} interface, and contain a <code>callID</code> and a <code>timestamp</code>.
 * Other than this, the JMSRequestSink does not care about the format of the sent messages, but will deliver them to the
 * RequestSink on the other side, where they must be handled.
 *
 * The messaging protocol between a JMSRequestSink and JMSRequestProxy is private between these entities, and should be
 * concidered transparent.
 *
 * The implementation will serialize messages to bytes. If upstream messages are above the configured maxMessageSize,
 * the JMSRequestSink will create an upload channel to establish a temporary queue for uploading, and will fragment the upload
 * message into multiple messages on the upload channel. This avoids very large JMS messages for upload.
 *
 * For download, the server RequestSink can choose to send multiple replies on the same reply channel, and can use this
 * to stream the results back to the client.
 *
 */
public class JMSRequestSink extends JMSBase implements RequestSink, RequestListener {

  private static final int DEFAULT_MAX_MESSAGE_SIZE = 65536;
  private static final Logger LOGGER = Logging.getLogger(JMSRequestSink.class);
  static final int DEFAULT_TTL = 1000;
  static final int DEFAULT_PRIORITY = 1;

  // variables

  private final ConcurrentHashMap<String, ClassLoaderSignalContextPair> requests = new ConcurrentHashMap<>();
  private final AtomicReference<ResponseListener> responseListener = new AtomicReference<>();
  private final int maxMessageSize;
  private final ProtocolVersion protocolVersion;

  private JMSRequestSink(List<JMSConnection> connections, String destinationName, long failbackInterval,
                         int timeToLive, int priority, boolean persistent,
                         int maxMessageSize, ProtocolVersion protocolVersion) {
    super(connections, destinationName, false,
            timeToLive, priority, persistent, false);
    this.maxMessageSize = maxMessageSize;
    this.protocolVersion = protocolVersion;
  }

  // **************** interface methods **************************

  public <T extends RequestContext> T signal(Message msg, final T signalContext, long maxWait) {
    //make sure message has callID set
    if (msg.getCallID() == null) {
      throw new IllegalArgumentException("Cannot accept message with no callID");
    }

    //if no context given by user, create a null context
    RequestContext ctx = ObjectUtils.ifNull(signalContext, NullRequestContext::new);

    //register for client-side notifications
    ctx.addListener(this);

    checkForFragmentationAndSignal(msg, ctx, maxWait);

    return signalContext;
  }

  public void close(String callID) {
    //close the specified request
    synchronized (this) {
      requests.remove(callID);
    }
  }

  @Override
  public void stopComponent() {
    ensureResponseListenerClosed();
    super.stopComponent();
  }

  @Override
  public void invalidate() {
    //invalidate this request sink, forcing reconnection
    //stop response listener thread, also closing down responseQueue and consumer
    ensureResponseListenerClosed();
    //perform normal invalidate
    super.invalidate();
  }

  // ****************** private methods ************************

  private void checkForFragmentationAndSignal(Message msg, RequestContext ctx, long maxWait) {
    try {
      byte[] messageBytes = JMSUtils.serialize(msg);
      String messageType = JMSRequestProxy.MESSAGE_TYPE_SIGNAL;
      //check if we need to fragment this request message
      if (messageBytes.length > maxMessageSize) {
        //if needing to fragment, replace signal context with a wrapper client upload context and send a channel request
        ctx = new ChannelUploadMessageContext(ctx, new ByteArrayInputStream(messageBytes), msg.getCallID(), maxMessageSize, protocolVersion);
        messageBytes = "channel upload request".getBytes();
        messageType = JMSRequestProxy.MESSAGE_TYPE_CHANNEL_REQUEST;
      }
      requests.putIfAbsent(msg.getCallID(), new ClassLoaderSignalContextPair(ctx, Thread.currentThread().getContextClassLoader()));
      sendMessage(messageBytes, msg.getCallID(), messageType, maxWait);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private synchronized void ensureResponseListenerClosedIfRequestsEmpty() {
    if (!requests.isEmpty()) return;
    ensureResponseListenerClosed();
  }

  private synchronized void ensureResponseListenerClosed() {
    ResponseListener l = responseListener.getAndSet(null);
    if (l != null) runInSeparateThread(l::close);
  }

  private void cleanResponseSinks() {
    for (Map.Entry<String, ClassLoaderSignalContextPair> e : requests.entrySet()) {
      ClassLoaderSignalContextPair c = e.getValue();
      if (c.requestContext.isClosed()) {
        requests.remove(e.getKey());
      }
    }
    ensureResponseListenerClosedIfRequestsEmpty();
  }

  private ResponseListener checkResponseListener() {
    return responseListener.updateAndGet(u -> u != null ? u : new ResponseListener());
  }

  private void sendMessage(byte[] messageBytes, String callID, String messageType, long lifeTime) {
    try {
      javax.jms.Message m = JMSUtils.createByteMessage(getSession(), messageBytes, protocolVersion);
      m.setJMSReplyTo(checkResponseListener().getResponseQueue());
      m.setJMSCorrelationID(callID);
      m.setStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE, messageType);
      m.setLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT, System.currentTimeMillis() + lifeTime);
      sendJMSMessage(m);
    } catch (Exception e) {
      throw handleMessagingException(e);
    }
  }

  private ClassLoaderSignalContextPair getCurrentCall(String callID) {
    if (callID == null) {
      LOGGER.warning("Message received without callID");
      return null;
    }
    ClassLoaderSignalContextPair ctx = requests.get(callID);
    if (ctx == null) {
      LOGGER.warning("Discarding signal response: " + callID);
    }
    return ctx;
  }

  @SuppressWarnings("UnusedReturnValue")
  private boolean handleResponse(javax.jms.Message message) throws JMSException {
    String responseType = message.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE);
    if (responseType == null) responseType = "N/A";
    switch (responseType) {
      case JMSRequestProxy.MESSAGE_TYPE_SIGNAL_RESPONSE:
        return handleSignalResponse(message);
      case JMSRequestProxy.MESSAGE_TYPE_CHANNEL_SETUP:
        return handleChannelSetup(message);
      case JMSRequestProxy.MESSAGE_TYPE_EXCEPTION:
        return handleSignalError(message);
      case JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED:
        return handleSignalEndOfStream(message);
      case JMSRequestProxy.MESSAGE_TYPE_EXTEND_WAIT:
        return handleSignalExtendWait(message);
      default:
        LOGGER.warning("Ignoring invalid response type: " + responseType);
        return false;
    }
  }

  private boolean handleSignalResponse(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    try (ClassLoaderContext ignored = ClassLoaderContext.of(ctx.classLoader)) {
      ctx.requestContext.addResponse(JMSUtils.extractObject(response));
    }
    return true;
  }

  private boolean handleChannelSetup(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    try {
      if (ctx.requestContext instanceof ChannelUploadMessageContext) {
        ChannelUploadMessageContext uploadCtx = (ChannelUploadMessageContext) ctx.requestContext;
        uploadCtx.upload(getSession(), response.getJMSReplyTo());
        return true;
      } else {
        LOGGER.warning("Received channel setup for a non-channel-upload client context: " + response.getJMSCorrelationID());
        return false;
      }
    } catch (NamingException e) {
      ctx.requestContext.notifyError(e);
      return false;
    }
  }

  private boolean handleSignalEndOfStream(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    ctx.requestContext.endOfStream();
    return true;
  }

  private boolean handleSignalExtendWait(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    ctx.requestContext.keepAlive(response.getLongProperty(JMSRequestProxy.PROPERTY_REQ_TIMEOUT));
    return true;
  }

  private boolean handleSignalError(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    if (JMSRequestProxy.MESSAGE_TYPE_STREAM_CLOSED.equals(response.getStringProperty(JMSRequestProxy.PROPERTY_MESSAGE_TYPE))) {
      ctx.requestContext.endOfStream();
    } else {
      try (ClassLoaderContext ignored = ClassLoaderContext.of(ctx.classLoader)) {
        ExceptionMessage em = JMSUtils.extractObject(response);
        //noinspection ThrowableResultOfMethodCallIgnored
        ctx.requestContext.notifyError(em.getException());
      }
    }
    return true;
  }

  /**
   * This listener listens for incoming reply messages and adds them to the correct responsehandler
   *
   * @author joakim
   */
  private class ResponseListener implements javax.jms.MessageListener, javax.jms.ExceptionListener {

    private TemporaryQueue responseQueue;
    private MessageConsumer responseConsumer;

    ResponseListener() throws MessagingException {
      try {
        responseQueue = getSession().createTemporaryQueue();
        responseConsumer = getSession().createConsumer(responseQueue);
        responseConsumer.setMessageListener(this);
        getConnection().addExceptionListener(this);
      } catch (Exception e) {
        LOGGER.warning(e, "Error setting up response listener");
        invalidate();
        throw new MessagingException("Error setting up response listener", e);
      }
    }

    TemporaryQueue getResponseQueue() {
      return responseQueue;
    }

    public void onException(JMSException e) {
      //run invalidation in another thread
      invalidateInSeparateThread();
      //notify all connected client contexts about error
      requests.values().forEach(ctx -> LambdaUtils.tryTo(() -> ctx.requestContext.notifyError(e)));
    }

    public void onMessage(javax.jms.Message message) {
      try {
        if (!JMSUtils.isCompatible(message)) {
          LOGGER.warning("Ignoring message of incompatible version");
          return;
        }
        handleResponse(message);
      } catch (Throwable e) {
        LOGGER.error(e, "Error receiving message");
      }

      //cleanup response sinks
      cleanResponseSinks();
    }

    public void close() {
      try {
        JMSUtils.removeMessageListenerAndClose(responseConsumer);
        JMSUtils.deleteTemporaryQueue(responseQueue);
        JMSUtils.removeExceptionListener(getConnection(), this);
      } catch (Exception e) {
        LOGGER.warning(e, "Error in close()");
      } finally {
        responseConsumer = null;
        responseQueue = null;
      }
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
    }

    public boolean isClosed() {
      return true;
    }

    public void notifyError(Throwable e) {
    }

    public void addListener(RequestListener listener) {
    }

    public void removeListener(RequestListener listener) {
    }
  }

  @SuppressWarnings("WeakerAccess")
  public static Builder builder() {
    return new Builder();
  }

  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class Builder {

    //fields
    private List<JMSConnection> connections;
    private String destinationName;
    private long failbackInterval;
    private int timeToLive = DEFAULT_TTL;
    private int priority = DEFAULT_PRIORITY;
    private boolean persistent = false;
    private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
    private ProtocolVersion protocolVersion = ProtocolVersion.V1;

    private Builder() {
    }

    public JMSRequestSink build() {
      if (protocolVersion == null) throw new IllegalArgumentException("protocolVersion not set");
      if (CollectionUtils.isEmpty(connections)) throw new IllegalArgumentException("No connections defined");
      if (destinationName == null) throw new IllegalArgumentException("No destination name provided");
      return new JMSRequestSink(connections, destinationName, failbackInterval, timeToLive, priority, persistent,
              maxMessageSize, protocolVersion);
    }

    //setters

    public Builder addConnection(JMSConnection c) {
      this.connections = ListUtils.addToList(this.connections, c);
      return this;
    }

    public Builder setConnections(List<JMSConnection> connections) {
      this.connections = connections;
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

    public Builder setPersistent(boolean persistent) {
      this.persistent = persistent;
      return this;
    }

    public Builder setMaxMessageSize(int maxMessageSize) {
      this.maxMessageSize = maxMessageSize;
      return this;
    }

    public Builder setProtocolVersion(ProtocolVersion protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
    }
  }


  //inner classes

  private class ClassLoaderSignalContextPair {
    private final RequestContext requestContext;
    private final ClassLoader classLoader;

    private ClassLoaderSignalContextPair(RequestContext requestContext, ClassLoader classLoader) {
      this.requestContext = requestContext;
      this.classLoader = classLoader;
    }
  }
}
