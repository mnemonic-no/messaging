package no.mnemonic.messaging.api;

/**
 * @author joakim
 */
public interface SignalContext extends MessageContext {

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
