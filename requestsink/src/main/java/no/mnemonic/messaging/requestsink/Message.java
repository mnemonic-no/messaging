package no.mnemonic.messaging.requestsink;

import java.io.Serializable;

/**
 * A message sent as a request or response.
 * A message must have a timestamp and callID, which are immutable.
 */
public interface Message extends Serializable {

  /**
   * @return the call ID of this message
   */
  String getCallID();

  /**
   * @return the message timestamp
   */
  long getMessageTimestamp();

}
