package no.mnemonic.messaging.api;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.ListUtils;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Common implementation for asynchronous response handling.
 * <p>
 * After making an asynchronous call, this responsehandler may be used for tracking responses from the called
 * components.
 */
public class SignalHandler implements SignalContext, CallIDAwareMessageContext {

  private static Clock clock = Clock.systemUTC();
  private static final Logger LOGGER = Logging.getLogger(SignalHandler.class);

  private final BlockingQueue<Message> responses = new LinkedBlockingDeque<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Throwable> error = new AtomicReference<>();
  private final Set<RequestListener> requestListeners = Collections.synchronizedSet(new HashSet<>());
  private final boolean allowKeepAlive;
  private final AtomicReference<String> callID = new AtomicReference<>();
  private final AtomicLong timeout = new AtomicLong();

  public SignalHandler() {
    this.allowKeepAlive = false;
  }

  public SignalHandler(boolean allowKeepAlive, String callID) {
    this.allowKeepAlive = allowKeepAlive;
    this.callID.set(callID);
  }

  //interface methods

  public String getCallID() {
    return callID.get();
  }

  public void setCallID(String callID) {
    this.callID.set(callID);
  }

  public void addListener(RequestListener listener) {
    requestListeners.add(listener);
  }

  public void removeListener(RequestListener listener) {
    requestListeners.remove(listener);
  }

  public boolean keepAlive(long until) {
    if (isClosed()) return false;
    if (allowKeepAlive) {
      if (timeout.getAndUpdate(prev -> until > prev ? until : prev) < until) {
        LOGGER.info("Keeping session open until %s", new Date(until));
      }
      return true;
    }
    return false;
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
      if (closed.get()) return false;
      responses.add(msg);
      //whenever receiving another response, this is an implicit 10sec keepalive
      keepAlive(clock.millis() + 10000);
      this.notifyAll();
    }
    return true;
  }

  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Wait for end-of-stream. Method will return true when end-of-stream is received.
   * It will return false if maxWait millis pass without receiving end-of-stream,
   * and will also return false if an error is received.
   *
   * @param maxWait do not wait longer than maxWait ms
   * @return true if end-of-stream is returned, false otherwise
   */
  public boolean waitForEndOfStream(long maxWait) {
    long localTimeout = clock.millis() + maxWait;
    try {
      synchronized (this) {
        //do not wait if EOS has already been received
        if (isClosed()) return true;
        //wait until timeout, but never longer
        while (clock.millis() < localTimeout) {
          this.wait(maxWait);
          if (isClosed() || hasReceivedError()) return isClosed();
        }
        //close and return true if timeout is exceeded (no keepalive is received)
        if (clock.millis() > this.timeout.get()) {
          close();
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
      requestListeners.forEach(l -> l.close(callID.get()));
    }
  }

  /**
   * Fetch the responses received so far, without delay.
   * Don't wait for additional responses.
   *
   * @return all responses currently received
   */
  public <T extends Message> Collection<T> getResponsesNoWait() {
    Collection<Message> result = new ArrayList<>();
    responses.drainTo(result);
    //noinspection unchecked
    return ListUtils.list(responses, v->(T)v);
  }

  /**
   * Wait for the next response to show up (since the last time this method was called).
   *
   * @param maxWait max millis to wait before returning
   * @return the first response to show up, or null if no responses were
   * recorded within maxWait millis.
   */
  public <T extends Message> T getNextResponse(long maxWait) {
    try {
      //noinspection unchecked
      return (T) responses.poll(maxWait, TimeUnit.MILLISECONDS);
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
  public <T extends Message> Collection<T> getResponses(long maxWait, int maxResults) {
    // determine timeout
    long timeout = clock.millis() + maxWait;
    if (maxWait == 0)
      timeout = 0;

    while (timeout > 0 && clock.millis() < timeout) {
      // if enough responses have come, return responses (so far)
      synchronized (this) {
        if (hasReceivedError())
          throw new RuntimeException(error.get());
        if (responses.size() >= maxResults)
          break;
        try {
          this.wait(timeout - clock.millis());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    //if timeout has passed, return responses received so far
    return getResponsesNoWait();
  }

  static void setClock(Clock clk) {
    clock = clk;
  }

}