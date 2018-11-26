package no.mnemonic.messaging.documentchannel.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.messaging.documentchannel.DocumentChannel;
import no.mnemonic.messaging.documentchannel.DocumentDestination;

import javax.jms.JMSException;
import javax.jms.Message;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

/**
 * JMS version of a document channel destination. The source is configured with a JMS session, which points to a JMS server/cluster
 * and a destination, which may be a topic or a queue.
 *
 * @param <T> document type
 */
public class JMSDocumentDestination<T> implements DocumentDestination<T> {

  private static final Logger LOGGER = Logging.getLogger(JMSDocumentDestination.class);

  private final Supplier<JMSSession> sessionProvider;
  private final Class<T> type;
  private final AtomicReference<JMSSession> currentSession = new AtomicReference<>();

  private final LongAdder sessionsCreated = new LongAdder();
  private final LongAdder producerExceptions = new LongAdder();

  private JMSDocumentDestination(Supplier<JMSSession> sessionProvider, Class<T> type) {
    this.sessionProvider = sessionProvider;
    this.type = type;
    if (!SetUtils.in(type, String.class, byte[].class)) {
      throw new IllegalArgumentException("Invalid type: " + type);
    }
  }

  @Override
  public DocumentChannel<T> getDocumentChannel() {
    return this::sendMessage;
  }

  @Override
  public void close() {
    closeSession();
  }

  //private methods

  private void sendMessage(T document) {
    try {
      JMSSession session = getSession();
      Message msg;
      if (type.equals(String.class) && document instanceof String) {
        msg = session.createTextMessage((String) document);
      } else if (type.equals(byte[].class) && document instanceof byte[]) {
        msg = session.createByteMessage((byte[]) document);
      } else {
        throw new IllegalStateException("Unexpected type: " + type);
      }
      session.getProducer().send(msg);
    } catch (JMSDocumentChannelException | JMSException e) {
      producerExceptions.increment();
      LOGGER.warning(e, "Error writing to JMS DocumentDestination");
      closeSession();
    }
  }

  private void closeSession() {
    currentSession.updateAndGet(session -> {
      ifNotNullDo(session, s -> tryTo(s::close));
      return null;
    });
  }

  private JMSSession getSession() throws JMSDocumentChannelException {
    try {
      return currentSession.updateAndGet(s -> ifNull(s, this::createSession));
    } catch (Exception e) {
      throw new JMSDocumentChannelException("Error creating new session", e);
    }
  }

  private JMSSession createSession() {
    sessionsCreated.increment();
    return sessionProvider.get();
  }

  public long getSessionsCreated() {
    return sessionsCreated.longValue();
  }

  public long getProducerExceptions() {
    return producerExceptions.longValue();
  }

  //builders

  public static <T>Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {

    //fields
    private Supplier<JMSSession> sessionProvider;
    private Class<T> type;

    public JMSDocumentDestination<T> build() {
      return new JMSDocumentDestination<>(sessionProvider, type);
    }

    //setters

    public Builder<T> setSessionProvider(Supplier<JMSSession> sessionProvider) {
      this.sessionProvider = sessionProvider;
      return this;
    }

    public Builder<T> setType(Class<T> type) {
      this.type = type;
      return this;
    }
  }

}
