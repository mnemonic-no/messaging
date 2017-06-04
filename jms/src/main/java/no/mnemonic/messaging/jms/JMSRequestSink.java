package no.mnemonic.messaging.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.api.*;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.TemporaryQueue;
import javax.naming.NamingException;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static no.mnemonic.messaging.jms.JMSRequestProxy.*;

/**
 * JMSRequestSink.java:
 * <ul>
 * <li>maxWait</li>
 * <li>jmsFacade</li>
 * <li>jmsDestination</li>
 * </ul>
 *
 * @author joakim Date: 17.des.2004 Time: 16:25:11
 * @version $Id$
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

  private final LongAdder requestCounter = new LongAdder();
  private final LongAdder channelUploadCounter = new LongAdder();

  private JMSRequestSink(List<JMSConnection> connections, String destinationName, long failbackInterval,
                         int timeToLive, int priority, boolean persistent,
                         int maxMessageSize, ProtocolVersion protocolVersion) {
    super(connections, destinationName, false,
            failbackInterval, timeToLive, priority, persistent, false);
    this.maxMessageSize = maxMessageSize;
    this.protocolVersion = protocolVersion;
  }

  // **************** interface methods **************************

  public <T extends SignalContext> T signal(Message msg, final T signalContext, long maxWait) {
    SignalContext ctx = signalContext;
    //if no context given by user, create a null context
    if (ctx == null) ctx = new NullSignalContext();

    //register for client-side notifications
    ctx.addListener(this);

    //check if message has any special interfaces to handle
    checkMessageInterfaces(msg, ctx);

    //make sure message has callID set
    if (msg.getCallID() == null) msg.setCallID(UUID.randomUUID().toString());

    // determine message labels
    String callID = msg.getCallID();

    if (protocolVersion == ProtocolVersion.V16) {
      checkForFragmentationAndSignal(msg, ctx, callID, maxWait);
    } else {
      // send request using old protocol (no fragmenting)
      requests.putIfAbsent(callID, new ClassLoaderSignalContextPair(ctx, Thread.currentThread().getContextClassLoader()));
      sendV13Message(msg, callID, MESSAGE_TYPE_SIGNAL, maxWait);
    }

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

  private void checkMessageInterfaces(Message msg, SignalContext ctx) {
    //check callID
    if (ctx instanceof CallIDAwareMessageContext) {
      CallIDAwareMessageContext callIDAwareContext = (CallIDAwareMessageContext) ctx;
      //make sure context has a callID set, preferably the message callID, else generate a random callID
      if (callIDAwareContext.getCallID() == null) {
        callIDAwareContext.setCallID(msg.getCallID() == null ? UUID.randomUUID().toString() : msg.getCallID());
      }
      //if message has no callID, set the context callID on the message
      if (msg.getCallID() == null) {
        msg.setCallID(callIDAwareContext.getCallID());
      }
    }
  }

  private void checkForFragmentationAndSignal(Message msg, SignalContext ctx, String callID, long maxWait) {
    try {
      byte[] messageBytes = JMSUtils.serialize(msg);
      String messageType = MESSAGE_TYPE_SIGNAL;
      //check if we need to fragment this request message
      if (messageBytes.length > maxMessageSize) {
        //if needing to fragment, replace signal context with a wrapper client upload context and send a channel request
        ctx = new ChannelUploadMessageContext(ctx, new ByteArrayInputStream(messageBytes), callID, maxMessageSize);
        messageBytes = "channel upload request".getBytes();
        messageType = MESSAGE_TYPE_CHANNEL_REQUEST;
      }
      requests.putIfAbsent(callID, new ClassLoaderSignalContextPair(ctx, Thread.currentThread().getContextClassLoader()));
      sendV16Message(messageBytes, callID, messageType, maxWait);
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
      if (c.signalContext.isClosed()) {
        requests.remove(e.getKey());
      }
    }
    ensureResponseListenerClosedIfRequestsEmpty();
  }

  private ResponseListener checkResponseListener() {
    return responseListener.updateAndGet(u -> u != null ? u : new ResponseListener());
  }

  private void sendV16Message(byte[] messageBytes, String callID, String messageType, long lifeTime) {
    try {
      javax.jms.Message m = JMSUtils.createByteMessage(getSession(), messageBytes);
      sendMessage(callID, messageType, lifeTime, m);
    } catch (Exception e) {
      throw handleMessagingException(e);
    }
  }

  private void sendV13Message(Serializable msg, String callID, String messageType, long lifeTime) {
    try {
      //noinspection deprecation
      javax.jms.Message m = JMSUtils.createObjectMessage(getSession(), msg);
      sendMessage(callID, messageType, lifeTime, m);
    } catch (Exception e) {
      throw handleMessagingException(e);
    }
  }

  private void sendMessage(String callID, String messageType, long lifeTime, javax.jms.Message m) throws JMSException {
    m.setJMSReplyTo(checkResponseListener().getResponseQueue());
    m.setJMSCorrelationID(callID);
    m.setStringProperty(PROPERTY_MESSAGE_TYPE, messageType);
    m.setLongProperty(PROPERTY_REQ_TIMEOUT, System.currentTimeMillis() + lifeTime);
    sendJMSMessage(m);
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
    String responseType = message.getStringProperty(PROPERTY_MESSAGE_TYPE);
    if (responseType == null) responseType = "N/A";
    switch (responseType) {
      case MESSAGE_TYPE_SIGNAL_RESPONSE:
        return handleSignalResponse(message);
      case MESSAGE_TYPE_CHANNEL_SETUP:
        return handleChannelSetup(message);
      case MESSAGE_TYPE_EXCEPTION:
        return handleSignalError(message);
      case MESSAGE_TYPE_STREAM_CLOSED:
        return handleSignalEndOfStream(message);
      case MESSAGE_TYPE_EXTEND_WAIT:
        return handleSignalExtendWait(message);
      default:
        LOGGER.warning("Ignoring invalid response type: " + responseType);
        return false;
    }
  }

  private boolean handleSignalResponse(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    try (JMSUtils.ClassLoaderContext ignored = JMSUtils.ClassLoaderContext.of(ctx.classLoader)) {
      ctx.signalContext.addResponse(JMSUtils.extractObject(response));
    }
    return true;
  }

  private boolean handleChannelSetup(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    try {
      if (ctx.signalContext instanceof ChannelUploadMessageContext) {
        ChannelUploadMessageContext uploadCtx = (ChannelUploadMessageContext) ctx.signalContext;
        uploadCtx.upload(getSession(), response.getJMSReplyTo());
        return true;
      } else {
        LOGGER.warning("Received channel setup for a non-channel-upload client context: " + response.getJMSCorrelationID());
        return false;
      }
    } catch (NamingException e) {
      ctx.signalContext.notifyError(e);
      return false;
    }
  }

  private boolean handleSignalEndOfStream(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    ctx.signalContext.endOfStream();
    return true;
  }

  private boolean handleSignalExtendWait(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    ctx.signalContext.keepAlive(response.getLongProperty(PROPERTY_REQ_TIMEOUT));
    return true;
  }

  private boolean handleSignalError(javax.jms.Message response) throws JMSException {
    ClassLoaderSignalContextPair ctx = getCurrentCall(response.getJMSCorrelationID());
    if (ctx == null) return false;
    if (MESSAGE_TYPE_STREAM_CLOSED.equals(response.getStringProperty(PROPERTY_MESSAGE_TYPE))) {
      ctx.signalContext.endOfStream();
    } else {
      try (JMSUtils.ClassLoaderContext ignored = JMSUtils.ClassLoaderContext.of(ctx.classLoader)) {
        ExceptionMessage em = JMSUtils.extractObject(response);
        //noinspection ThrowableResultOfMethodCallIgnored
        ctx.signalContext.notifyError(em.getException());
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
      requests.values().forEach(ctx -> LambdaUtils.tryTo(() -> ctx.signalContext.notifyError(e)));
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

  private static class NullSignalContext implements SignalContext {
    private NullSignalContext() {
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
    private ProtocolVersion protocolVersion = ProtocolVersion.V13;

    public JMSRequestSink build() {
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
    private final SignalContext signalContext;
    private final ClassLoader classLoader;

    private ClassLoaderSignalContextPair(SignalContext signalContext, ClassLoader classLoader) {
      this.signalContext = signalContext;
      this.classLoader = classLoader;
    }
  }
}
