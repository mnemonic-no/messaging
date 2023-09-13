package no.mnemonic.messaging.requestsink;

public interface ResponseListener {

  /**
   * Notify server that this response has been accepted,
   * releasing buffer capacity on the client.
   */
  void responseAccepted();

}
