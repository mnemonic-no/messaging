package no.mnemonic.messaging.requestsink;

/**
 * @author joakim
 */
public interface RequestListener {

  /**
   * Client-side notification that the request is closed
   *
   * @param callID the ID of the closed request
   */
  void close(String callID);

}
