package no.mnemonic.messaging.requestsink;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;

import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;

/**
 * Common implementation for asynchronous response handling.
 * <p>
 * After making an asynchronous call, this requesthandler may be used for tracking responses
 * from the requestsink.
 */
@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public class RequestHandler implements RequestContext {

  private static final int KEEPALIVE_PERIOD = 10000;
  private static final int DEFAULT_RESPONSE_QUEUE_SIZE = 100;
  private static final int DEFAULT_ADD_RESPONSE_MAX_WAIT_SECONDS = 30;

  private static Clock clock = Clock.systemUTC();
  private static final Logger LOGGER = Logging.getLogger(RequestHandler.class);

  private final BlockingQueue<Response> responses;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Throwable> error = new AtomicReference<>();
  private final Set<RequestListener> requestListeners = Collections.synchronizedSet(new HashSet<>());
  private final boolean allowKeepAlive;
  private final String callID;
  private final AtomicLong timeout = new AtomicLong();
  private final int addResponseMaxWaitSeconds;

  /**
   * Initialize a RequestHandler with an unbounded internal buffering queue.
   * If using JMSRequestSink, clients should not switch to use bounded response buffer queue unless
   * they have enabled the V4 JMSRequestSink protocol.
   *
   * @param allowKeepAlive if allowKeepalive, received keepalive messages and responses will cause call timeout to be extended
   * @param callID         the ID of the call this handler represents
   * @param maxWait        the maximum time in milliseconds before this call times out (unless keepAlive is received)
   * @deprecated Use builder instead
   */
  @Deprecated
  public RequestHandler(boolean allowKeepAlive, String callID, long maxWait) {
    this(allowKeepAlive, callID, maxWait, 0, DEFAULT_ADD_RESPONSE_MAX_WAIT_SECONDS);
  }

  /**
   * Initialize a RequestHandler
   *
   * @param allowKeepAlive            if allowKeepalive, received keepalive messages and responses will cause call timeout to be extended
   * @param callID                    the ID of the call this handler represents
   * @param maxWait                   the maximum time in milliseconds before this call times out (unless keepAlive is received)
   * @param responseQueueSize         the capacity of the internal buffering queue. A value of 0 means unbounded.
   *                                  If bounding the queue, make sure the underlying RequestSink protocol supports
   *                                  using segment window, with a segment window size smaller than the responseQueueSize.
   * @param addResponseMaxWaitSeconds
   */
  private RequestHandler(boolean allowKeepAlive, String callID, long maxWait, int responseQueueSize, int addResponseMaxWaitSeconds) {
    this.allowKeepAlive = allowKeepAlive;
    this.callID = callID;
    this.addResponseMaxWaitSeconds = addResponseMaxWaitSeconds;
    this.timeout.set(clock.millis() + maxWait);
    //default
    if (responseQueueSize == 0) {
      responses = new LinkedBlockingDeque<>();
    } else {
      responses  = new LinkedBlockingDeque<>(responseQueueSize);
    }
  }

  /**
   * Convenience method to create a RequestHandler and submit a signal bound to this handler as a RequestContext
   *
   * @param sink           the sink to send to
   * @param msg            the message to send
   * @param allowKeepAlive if keepalive should be allowed
   * @param maxWait        max initial milliseconds before request times out (if no keepalive)
   * @return the RequestHandler which is used as RequestContext
   * @deprecated Use the {@link #builder()} to create the RequestHandler, and invoke {@link RequestSink#signal(Message, RequestContext, long)} directly.
   */
  @Deprecated
  public static RequestHandler signal(RequestSink sink, Message msg, boolean allowKeepAlive, long maxWait) {
    if (sink == null) throw new IllegalArgumentException("RequestSink cannot be null");
    if (msg == null) throw new IllegalArgumentException("Message cannot be null");
    if (maxWait <= 0) throw new IllegalArgumentException("MaxWait must be a positive integer");
    RequestHandler handler =  RequestHandler.builder()
        .setAllowKeepAlive(allowKeepAlive)
        .setCallID(msg.getCallID())
        .setMaxWait(maxWait)
        .build();
    sink.signal(msg, handler, maxWait);
    if (LOGGER.isDebug()) {
      LOGGER.debug(">> signal [callID=%s msg=%s allowKeepalive=%s maxWait=%d]", msg.getCallID(), msg.getClass(), allowKeepAlive, maxWait);
    }
    return handler;
  }

  //interface methods

  public void addListener(RequestListener listener) {
    requestListeners.add(listener);
  }

  public void removeListener(RequestListener listener) {
    requestListeners.remove(listener);
  }

  public boolean keepAlive(long until) {
    if (isClosed() || !allowKeepAlive) {
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< keepAlive rejected[callID=%s  until=%s]", callID, new Date(until));
      }
      return false;
    }
    if (timeout.getAndUpdate(prev -> until > prev ? until : prev) < until) {
      LOGGER.debug("Keeping session open [callID=%s until=%s]", callID, new Date(until));
    }
    return true;
  }

  public void endOfStream() {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< endOfStream [callID=%s]", callID);
    }
    close();
  }

  public void notifyError(Throwable e) {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< notifyError [callID=%s throwable=%s]", callID, e.getClass());
    }
    error.set(e);
    synchronized (this) {
      this.notifyAll();
    }
    close();
  }

  @Override
  public void notifyClose() {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< notifyClose [callID=%s]", callID);
    }
    list(requestListeners).forEach(l -> LambdaUtils.tryTo(
            () -> l.close(callID),
            e -> LOGGER.warning(e, "Error invoking RequestListener")
    ));
  }

  @Override
  public boolean addResponse(Message msg) {
    return this.addResponse(msg, ()->{});
  }

  @Override
  public boolean addResponse(Message msg, ResponseListener responseListener) {

    if (isClosed()) {
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< addResponse rejected [callID=%s]", callID);
      }
      return false;
    }
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< addResponse [callID=%s, responseQueueSize=%d]", callID, responses.size());
    }
    try {
      if (!responses.offer(new Response(msg, responseListener), addResponseMaxWaitSeconds, TimeUnit.SECONDS)) {
        LOGGER.error("Timed out waiting for client queue capacity, client has been terminated");
        abort();
        responses.clear();
      }
    } catch (InterruptedException e) {
      throw new MessagingInterruptedException(e);
    }
    //whenever receiving another response, this is an implicit 10sec keepalive
    keepAlive(clock.millis() + KEEPALIVE_PERIOD);
    synchronized (this) {
      this.notifyAll();
    }
    return true;
  }

  @Override
  public boolean isClosed() {
    //close and return true if handler timeout is exceeded
    if (clock.millis() > this.timeout.get()) {
      close();
    }
    return closed.get();
  }

  //public methods

  /**
   * @return the callID this handler is bound to
   */
  public String getCallID() {
    return callID;
  }

  /**
   * Wait for end-of-stream. Method will return true when end-of-stream is received, or timeout has occurred.
   * If handler accepts keepalives, this means that waitForEndOfStream may return false when waiting for the configured
   * maxwait millis, because timeout is extended. However, method will return (true or false) no later than maxWait.
   *
   * @param maxWait do not wait longer than maxWait ms
   * @return true if handler is closed (end of stream or timeout), false otherwise
   */
  public boolean waitForEndOfStream(long maxWait) {
    long localTimeout = clock.millis() + maxWait;
    try {
      //do not wait if EOS has already been received
      if (isClosed()) return true;
      //wait until timeout, but never longer
      long now;
      while ((now = clock.millis()) < localTimeout) {
        synchronized (this) {
          this.wait(localTimeout - now);
        }
        if (isClosed() || hasReceivedError()) return isClosed();
      }
      return isClosed();
    } catch (InterruptedException e) {
      LOGGER.warning(e, "Interrupted");
      close();
      return isClosed();
    }
  }

  //public methods

  /**
   * @return true if the response stream has received an error
   */
  public boolean hasReceivedError() {
    return error.get() != null;
  }

  /**
   * Close this handler, closing all listening resources.
   * Further responses to this signal will be ignored
   */
  public void close() {
    boolean wasClosed = closed.getAndSet(true);
    synchronized (this) {
      this.notifyAll();
    }
    if (!wasClosed) {
      if (LOGGER.isDebug()) {
        LOGGER.debug("# close [callID=%s]", callID);
      }
      list(requestListeners).forEach(l -> LambdaUtils.tryTo(
              () -> l.close(callID),
              e -> LOGGER.warning(e, "Error invoking RequestListener")
      ));
    }
  }

  /**
   * Abort this handler, notifying all listening resources.
   * This will also close the handler,so further signals are ignored.
   */
  public void abort() {
    if (isClosed()) return;
    list(requestListeners).forEach(l -> LambdaUtils.tryTo(
            () -> l.abort(callID),
            e -> LOGGER.warning(e, "Error invoking RequestListener")
    ));
    close();
  }

  /**
   * Signal unexpected timeout to request listeners
   */
  public void timeout() {
    list(requestListeners).forEach(l -> LambdaUtils.tryTo(l::timeout));
  }

  /**
   * Fetch the responses received so far, without delay.
   * Don't wait for additional responses.
   *
   * @return all responses currently received
   */
  public <T extends Message> Collection<T> getResponsesNoWait() throws InvocationTargetException {
    checkIfReceivedError();
    Collection<Response> result = new ArrayList<>();
    responses.drainTo(result);
    return result.stream()
            .map(r->{
              ifNotNullDo(r.listener, ResponseListener::responseAccepted);
              return (T)(r.message);
            })
            .collect(Collectors.toList());
  }


  /**
   * Wait for the next response to show up (since the last time this method was called).
   * Similar to {@link #getNextResponse(long)}, but will continue waiting until a response is received, or until end of stream/stream timeout.
   * If stream is kept alive, this may wait indefinetely
   *
   * @return the first response to show up, or null if the stream was closed before any responses show up
   */
  public <T extends Message> T getNextResponse() throws InvocationTargetException {
    T response = getNextResponse(1000);
    while (response == null && !isClosed()) {
      response = getNextResponse(1000);
    }
    return response;
  }

  /**
   * Wait for the next response to show up (since the last time this method was called).
   *
   * @param maxWait max millis to wait before returning
   * @return the first response to show up, or null if no responses were
   * recorded within maxWait millis or end of stream.
   */
  public <T extends Message> T getNextResponse(long maxWait) throws InvocationTargetException {
    try {
      checkIfReceivedError();
      if (responses.isEmpty()) {
        if (isClosed()) return null;
        synchronized (this) {
          this.wait(maxWait);
        }
        checkIfReceivedError();
      }
      //noinspection unchecked
      Response r = responses.poll();
      if (r == null) return null;
      ifNotNullDo(r.listener, ResponseListener::responseAccepted);
      return (T) (r.message);
    } catch (InterruptedException e) {
      throw new MessagingInterruptedException(e);
    }
  }

  /**
   * Same as getResponses(long), but will return immediately when maxResults results are returned,
   * without waiting for more results.
   *
   * @param maxWait    max millis to wait before returning.
   * @param maxResults max results to receive before returning.
   * @return a collection with the results received up until maxWait millis,
   * or maxResults results, whatever happens first.
   */
  public <T extends Message> Collection<T> getResponses(long maxWait, int maxResults) throws InvocationTargetException {
    // determine timeout
    long timeout = maxWait > 0 ? clock.millis() + maxWait : 0;
    //wait for timeout (or enough responses)
    while (timeout > 0 && clock.millis() < timeout) {
      // if enough responses have come, return responses (so far)
      checkIfReceivedError();
      if (responses.size() >= maxResults || isClosed()) {
        return getResponsesNoWait();
      }
      try {
        synchronized (this) {
          this.wait(Math.max(1, timeout - clock.millis()));
        }
      } catch (InterruptedException e) {
        throw new MessagingInterruptedException(e);
      }
    }
    //if timeout has passed, return responses received so far
    return getResponsesNoWait();
  }

  //private methods

  private void checkIfReceivedError() throws InvocationTargetException {
    if (hasReceivedError()) {
      throw new InvocationTargetException(error.get());
    }
  }

  private static class Response {
    private final Message message;
    private final ResponseListener listener;

    public Response(Message message, ResponseListener listener) {
      this.message = message;
      this.listener = listener;
    }
  }

  //setters

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    //fields
    private boolean allowKeepAlive;
    private String callID;
    private long maxWait;
    private int responseQueueSize = DEFAULT_RESPONSE_QUEUE_SIZE;
    private int addResponseMaxWaitSeconds = DEFAULT_ADD_RESPONSE_MAX_WAIT_SECONDS;

    public RequestHandler build() {
      return new RequestHandler(allowKeepAlive, callID, maxWait, responseQueueSize, addResponseMaxWaitSeconds);
    }

    //setters

    public Builder setAddResponseMaxWaitSeconds(int addResponseMaxWaitSeconds) {
      this.addResponseMaxWaitSeconds = addResponseMaxWaitSeconds;
      return this;
    }

    public Builder setAllowKeepAlive(boolean allowKeepAlive) {
      this.allowKeepAlive = allowKeepAlive;
      return this;
    }

    public Builder setCallID(String callID) {
      this.callID = callID;
      return this;
    }

    public Builder setMaxWait(long maxWait) {
      this.maxWait = maxWait;
      return this;
    }

    public Builder setResponseQueueSize(int responseQueueSize) {
      this.responseQueueSize = responseQueueSize;
      return this;
    }

  }

  //for testing only
  static void setClock(Clock clock) {
    RequestHandler.clock = clock;
  }
}