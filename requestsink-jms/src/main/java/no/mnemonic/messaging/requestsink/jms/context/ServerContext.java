package no.mnemonic.messaging.requestsink.jms.context;

/**
 * A ServerContext represents the server side of a client/server call.
 */
public interface ServerContext {

  /**
   *
   * @return true if this context has been closed
   */
  boolean isClosed();

  /**
   * Request that this context is aborted. No more responses should be allowed
   * Ongoing execution should be interrupted.
   */
  void abort();

  /**
   * Acknowledgement from client that a response has been processed at the client
   */
  void acknowledgeResponse();
}
