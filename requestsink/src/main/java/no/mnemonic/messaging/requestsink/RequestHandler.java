package no.mnemonic.messaging.requestsink;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;

import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Common implementation for asynchronous response handling.
 * <p>
 * After making an asynchronous call, this requesthandler may be used for tracking responses
 * from the requestsink.
 */
@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public class RequestHandler implements RequestContext {

  static final int KEEPALIVE_PERIOD = 10000;
  private static Clock clock = Clock.systemUTC();
  private static final Logger LOGGER = Logging.getLogger(RequestHandler.class);

  private final BlockingQueue<Message> responses = new LinkedBlockingDeque<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Throwable> error = new AtomicReference<>();
  private final Set<RequestListener> requestListeners = Collections.synchronizedSet(new HashSet<>());
  private final boolean allowKeepAlive;
  private final String callID;
  private final AtomicLong timeout = new AtomicLong();

  public RequestHandler(boolean allowKeepAlive, String callID, long maxWait) {
    this.allowKeepAlive = allowKeepAlive;
    this.callID = callID;
    this.timeout.set(clock.millis() + maxWait);
  }

  public static RequestHandler signal(RequestSink sink, Message msg, boolean allowKeepAlive, long maxWait) {
    if (sink == null) throw new IllegalArgumentException("RequestSink cannot be null");
    if (msg == null) throw new IllegalArgumentException("Message cannot be null");
    if (maxWait <= 0) throw new IllegalArgumentException("MaxWait must be a positive integer");
    RequestHandler handler = new RequestHandler(allowKeepAlive, msg.getCallID(), maxWait);
    sink.signal(msg, handler, maxWait);
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
    if (isClosed() || !allowKeepAlive) return false;
    if (timeout.getAndUpdate(prev -> until > prev ? until : prev) < until) {
      LOGGER.info("Keeping session open until %s", new Date(until));
    }
    return true;
  }

  public void endOfStream() {
    close();
  }

  public void notifyError(Throwable e) {
    synchronized (this) {
      error.set(e);
      this.notifyAll();
    }
    close();
  }

  public boolean addResponse(Message msg) {
    synchronized (this) {
      if (isClosed()) return false;
      responses.add(msg);
      //whenever receiving another response, this is an implicit 10sec keepalive
      keepAlive(clock.millis() + KEEPALIVE_PERIOD);
      this.notifyAll();
    }
    return true;
  }

  public boolean isClosed() {
    //close and return true if handler timeout is exceeded
    if (clock.millis() > this.timeout.get()) {
      close();
    }
    return closed.get();
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
      synchronized (this) {
        //do not wait if EOS has already been received
        if (isClosed()) return true;
        //wait until timeout, but never longer
        long now;
        while ((now = clock.millis()) < localTimeout) {
          this.wait(localTimeout - now);
          if (isClosed() || hasReceivedError()) return isClosed();
        }
        return isClosed();
      }
    } catch (InterruptedException e) {
      LOGGER.warning(e, "Interrupted");
      return isClosed();
    }
  }

  //public methods

  /**
   * @return true if the response stream has received an error
   */
  public boolean hasReceivedError() {
    synchronized (this) {
      return error.get() != null;
    }
  }

  /**
   * Close this handler, closing all listening resources.
   * Further responses to this signal will be ignored
   */
  public void close() {
    synchronized (this) {
      closed.set(true);
      this.notifyAll();
      requestListeners.forEach(l -> LambdaUtils.tryTo(
              () -> l.close(callID),
              e -> LOGGER.warning(e, "Error invoking RequestListener")
      ));
    }
  }

  /**
   * Fetch the responses received so far, without delay.
   * Don't wait for additional responses.
   *
   * @return all responses currently received
   */
  public <T extends Message> Collection<T> getResponsesNoWait() throws InvocationTargetException {
    checkIfReceivedError();
    Collection<Message> result = new ArrayList<>();
    responses.drainTo(result);
    //noinspection unchecked
    return ListUtils.list(result, v -> (T) v);
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
      synchronized (this) {
        checkIfReceivedError();
        if (responses.isEmpty()) {
          if (isClosed()) return null;
          this.wait(maxWait);
          checkIfReceivedError();
        }
        //noinspection unchecked
        return (T) responses.poll();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
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
      synchronized (this) {
        checkIfReceivedError();
        if (responses.size() >= maxResults || isClosed()) {
          return getResponsesNoWait();
        }
        try {
          this.wait(Math.max(1, timeout - clock.millis()));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    //if timeout has passed, return responses received so far
    return getResponsesNoWait();
  }

  private void checkIfReceivedError() throws InvocationTargetException {
    if (hasReceivedError()) {
      throw new InvocationTargetException(error.get());
    }
  }

}