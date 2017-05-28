package no.mnemonic.messaging.api;

/**
 * TransactedObjectSource.java:
 * <p>
 * This is an object requestSource which is transacted, i.e.
 * it must resend non-commited objects after a restart.
 *
 * @author joakim
 *         Date: 06.okt.2004
 *         Time: 07:59:19
 * @version $Id$
 */
public interface TransactedObjectSource<T> extends ObjectSource<T> {

  /**
   * Requests that this source starts a transaction for the objects it provides.
   *
   * @return a transaction which can commit the provided objects. The method may not return null.
   */
  Transaction requestSourceTransaction();

}
