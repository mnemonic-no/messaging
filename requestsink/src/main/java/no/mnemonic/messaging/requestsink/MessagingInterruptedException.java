package no.mnemonic.messaging.requestsink;

public class MessagingInterruptedException extends MessagingException {

  private static final long serialVersionUID = -4452057462415988594L;

  public MessagingInterruptedException() {
  }

  public MessagingInterruptedException(String s) {
    super(s);
  }

  public MessagingInterruptedException(Throwable throwable) {
    super(throwable);
  }

  public MessagingInterruptedException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
