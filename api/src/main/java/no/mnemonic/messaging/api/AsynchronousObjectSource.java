package no.mnemonic.messaging.api;

/**
 * Extention of an object source, receiving (and possibly actively pushing) messages
 *
 * @author joakim
 */
public interface AsynchronousObjectSource<T> {

  /**
   * Register this listener to receive messages when they arrive
   *
   * @param listener listener to send objects to
   */
  void addObjectListener(ObjectListener<T> listener);

  /**
   * Unregister this listener
   *
   * @param listener listener to unregister
   */
  void removeObjectListener(ObjectListener<T> listener);

  /**
   * Register this listener to receive errors in object source
   *
   * @param listener listener to send errors to
   */
  void addErrorListener(ErrorListener listener);

  /**
   * Unregister this listener
   *
   * @param listener listener to unregister
   */
  void removeErrorListener(ErrorListener listener);

}
