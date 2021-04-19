package no.mnemonic.messaging.requestsink.jms.util;

public class ReassemblyMissingFragmentException extends Exception {

  private static final long serialVersionUID = 7028950464759979366L;

  public ReassemblyMissingFragmentException(String msg) {
    super(msg);
  }
}
