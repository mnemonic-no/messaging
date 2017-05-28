package no.mnemonic.messaging.api;

/**
 * Interface for message contexts which are message ID aware
 *
 * @author joakim
 */
public interface CallIDAwareMessageContext extends MessageContext {

  /**
   * @return the ID of the current request
   */
  String getCallID();

  /**
   * Set the CallID to use for this message context
   *
   * @param callID callID to set on context
   */
  void setCallID(String callID);
}
