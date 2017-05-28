package no.mnemonic.messaging.api;

import java.io.Serializable;

/**
 * A Message object incorporates the fact that the message is sent from someone
 * to someone else.
 *
 * Any object may be a message, but the message should have a requestSource identifier and a destination identifier.
 *
 */
public interface Message extends Serializable {

  /**
   * @return the call ID of this message, if set
   */
  String getCallID();

  /**
   * Override the ID of this call
   *
   * @param callID new ID of this call
   */
  void setCallID(String callID);

  /**
   * @return the message timestamp
   */
  long getMessageTimestamp();

}
