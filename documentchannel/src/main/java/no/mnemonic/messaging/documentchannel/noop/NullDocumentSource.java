package no.mnemonic.messaging.documentchannel.noop;

import no.mnemonic.messaging.documentchannel.DocumentBatch;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import no.mnemonic.messaging.documentchannel.DocumentChannelSubscription;
import no.mnemonic.messaging.documentchannel.DocumentSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Empty implementation of a DocumentSource, which does absolutely nothing.
 * However, all methods are implemented in a null-safe way.
 *
 * @param <T> the channel document type
 */
public class NullDocumentSource<T> implements DocumentSource<T> {

  @Override
  public DocumentChannelSubscription createDocumentSubscription(DocumentChannelListener<T> documentChannelListener) {
    return () -> {
      //no nothing
    };
  }

  @Override
  public DocumentBatch<T> poll(long l, TimeUnit timeUnit) {
    return new DocumentBatch<T>() {
      @Override
      public Collection<T> getDocuments() {
        return new ArrayList<>();
      }

      @Override
      public void acknowledge() {
        //do nothing
      }
    };
  }

  @Override
  public void close() {
    //do nothing
  }
}