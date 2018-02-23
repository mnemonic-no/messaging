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

  /**
   * Client side notification that an unexpected timeout occurred.
   * The listener may act to avoid future errors (such as invalid return path or connection error)
   */
  void timeout();

}
