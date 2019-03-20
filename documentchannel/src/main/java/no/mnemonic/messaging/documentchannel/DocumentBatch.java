package no.mnemonic.messaging.documentchannel;

import java.util.Collection;
import java.util.Iterator;

/**
 * A batch of documents, returned by {@link DocumentSource#poll}
 *
 * @param <T> document type
 */
public interface DocumentBatch<T> extends Iterable<T> {

  /**
   * @return documents in batch
   */
  Collection<T> getDocuments();

  /**
   * acknowledge that this batch is handled
   */
  void acknowledge();

  @Override
  default Iterator<T> iterator() {
    return getDocuments().iterator();
  }
}
