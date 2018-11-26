package no.mnemonic.messaging.documentchannel.jms;

class JMSDocumentChannelException extends Exception {

  JMSDocumentChannelException(String message) {
    super(message);
  }

  JMSDocumentChannelException(String message, Throwable cause) {
    super(message, cause);
  }

  JMSDocumentChannelException(Throwable cause) {
    super(cause);
  }
}
