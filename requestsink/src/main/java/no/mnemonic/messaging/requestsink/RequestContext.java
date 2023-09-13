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
   * Notify signal context that server has closed this request.
   * The request may already be closed on the client side.
   */
  void notifyClose();

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
   * @deprecated Use {@link #addResponse(Message, ResponseListener)}
   */
  @Deprecated
  boolean addResponse(Message msg);

  /**
   * Add a signal response to give back to requesting client, with a acknoeldgement listener to notify when the response
   * has been accepted.
   *
   * When using this method, clients MUST invoke {@link ResponseListener#responseAccepted()} when the response has been handled,
   * else the sender may starve the client once the sender has sent responses enough to fill the agreed client buffer.
   *
   * @param msg the response message to add
   * @param responseListener listener to acknowledge to when the response has been accepted.
   * @return true if the response message was accepted, false if it was discarded
   */
  boolean addResponse(Message msg, ResponseListener responseListener);

  /**
   * Signal end of stream (the current context holder will not provide any more data).
   */
  void endOfStream();

}
