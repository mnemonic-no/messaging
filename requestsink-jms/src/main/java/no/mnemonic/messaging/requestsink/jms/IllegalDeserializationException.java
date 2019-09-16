package no.mnemonic.messaging.requestsink.jms;

import java.io.IOException;

/**
 * Exception class to handle illegal deserialization.
 */
public class IllegalDeserializationException extends IOException {

  public IllegalDeserializationException(String message) {
    super(message);
  }

}

