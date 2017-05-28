package no.mnemonic.messaging.api;

public class MessagingException extends RuntimeException {

  static final long serialVersionUID = 6573851470802615116L;

  public MessagingException() {
  }

  public MessagingException(String s) {
    super(s);
  }

  public MessagingException(Throwable throwable) {
    super(throwable);
  }

  public MessagingException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
