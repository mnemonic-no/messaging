package no.mnemonic.messaging.requestsink;

/**
 * A request context is the asynchronous interface for working with a pending request.
 * The context must be provided when sending a signal, and the RequestSink is obligated to send all
 * replies to this context. The context will also be notified if the RequestSink closes the request, or notifies about an error.
 */
public interface RequestContext {

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
   * Add a {@link RequestListener} to context. A context implementation should notify any registered listeners
   * about relevant events.
   *
   * @param listener listener to add
   *
   */
  void addListener(RequestListener listener);

  /**
   * Remove a RequestListener from context
   *
   * @param listener listener to remove
   */
  void removeListener(RequestListener listener);

  /**
   * Add a signal response to give back to requesting client
   *
   * @param msg the response message to add
   * @return true if the response message was accepted, false if it was discarded
   */
  boolean addResponse(Message msg);

  /**
   * Signal end of stream (the current context holder will not provide any more data).
   */
  void endOfStream();

}
