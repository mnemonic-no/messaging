package no.mnemonic.messaging.api;

/**
 * A transaction is a formal contract between a set of entities.
 * Whatever happens within a transaction should be reversible until the controlling
 * party has called commit. Calling rollback should return the system to the state it
 * had after the previous commit, or before the transaction started.
 */
public interface Transaction {

  /**
   * Persist all changes made from the start of the transaction and up to this time.
   * After this, changes done before the last commit should be permanent.
   */
  void commit();

  /**
   * Cancel all changes since the start of the transaction or the last commit.
   * After this, the system should be in the same state it had before the previous commit,
   * or the start of the transaction.
   */
  void rollback();
}
