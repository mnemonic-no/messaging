package no.mnemonic.messaging.api;

/**
 * Interface for response sinks forwarded to downstream requestsink
 *
 * @author joakim
 */
public interface MessageContext {

  /**
   * @return true if this request is closed down (will not accept any more responses)
   */
  boolean isClosed();

  /**
   * Request that this request should be kept alive a bit longer
   *
   * @param until Ask this request to keep open at least until this timestamp
   * @return true if the request is granted (will not guarantee that endpoint clients keep this up)
   */
  boolean keepAlive(long until);

  /**
   * Notify signal context that an error has occurred while waiting for data
   *
   * @param e exception that was caught
   */
  void notifyError(Throwable e);

  /**
   * Add a RequestListener to context
   *
   * @param listener listener to add
   */
  void addListener(RequestListener listener);

  /**
   * Remove a RequestListener from context
   *
   * @param listener listener to remove
   */
  void removeListener(RequestListener listener);


}
