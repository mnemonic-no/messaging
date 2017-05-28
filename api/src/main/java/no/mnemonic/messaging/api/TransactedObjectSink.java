package no.mnemonic.messaging.api;

/**
 * TransactedObjectSink.java:
 *
 * This is an object mockSink which is transacted, i.e.
 * it must cope with non-commited objects being resent after a restart.
 *
 * @author joakim
 *         Date: 06.okt.2004
 *         Time: 08:00:45
 * @version $Id$
 */
public interface TransactedObjectSink<T> extends ObjectSink<T> {

  /**
   * Requests that this sick starts a transaction for its delivered objects.
   *
   * @return a transaction which can commit the delivered objects. The method may not return null.
   */
  Transaction requestSinkTransaction();

}
