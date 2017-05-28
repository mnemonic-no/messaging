package no.mnemonic.messaging.api;

/**
 * Interface for receiving messages asynchronously
 *
 * @author joakim
 */
public interface ObjectListener<T> {

  /**
   * Called whenever a message is received
   *
   * @param msg received message
   */
  void receiveObject(T msg);

}
