package no.mnemonic.messaging.requestsink;

import java.io.Serializable;

/**
 * A message sent as a request or response.
 * A message must have a timestamp and callID, which are immutable.
 */
public interface Message extends Serializable {

  int DEFAULT_RESPONSE_WINDOW_SIZE = 50;

  /**
   * Allow client to set a priority hint on the message.
   * Messages with priority bulk should be processed later than messages with priority standard.
   * Expedite messages should be pushed to the front of the queue.
   * The implementation of this is depending on underlying implementation.
   */
  enum Priority {
    bulk, standard, expedite
  }

  default Priority getPriority() {
    return Priority.standard;
  }

  /**
   * @return the call ID of this message
   */
  String getCallID();

  /**
   * @return the message timestamp
   */
  long getMessageTimestamp();

  /**
   * @return the response window size, indicating the amount of responses the sender should send before receiving acknowledgement.
   * Requires protocol version V4.
   */
  default int getResponseWindowSize() {
    return DEFAULT_RESPONSE_WINDOW_SIZE;
  }

}
