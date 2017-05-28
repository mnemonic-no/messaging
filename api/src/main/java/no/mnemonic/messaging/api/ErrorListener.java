package no.mnemonic.messaging.api;

/**
 * Interface for receiving messages asynchronously
 *
 * @author joakim
 */
public interface ErrorListener {

  /**
   * Notify signal context that an error has occurred while waiting for data
   *
   * @param e exception that was caught
   */
  void notifyError(Exception e);

}
