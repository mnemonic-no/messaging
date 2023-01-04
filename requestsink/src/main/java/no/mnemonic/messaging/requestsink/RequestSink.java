package no.mnemonic.messaging.requestsink;

/**
 * @author joakim
 */
public interface RequestSink {

  /**
   * Send an asyncronous call. The method should return immediately.
   * Any replies for this call back to this entity is forwarded to the given signal context,
   * until the given time is elapsed, or the request is closed.
   *
   * @param <T>           context type
   * @param msg           message to send
   * @param signalContext a signal context to forward replies to, or null if replies are ignored
   * @param maxWait       the maximum lifetime of this request, before closing
   * @return the signal context
   */
  <T extends RequestContext> T signal(Message msg, T signalContext, long maxWait);

  /**
   * Signal the abortion of a call. The sink delivers no guarantees that the call can be aborted.
   * @param callID the callID of the call to abort
   */
  void abort(String callID);
}
