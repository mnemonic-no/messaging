package no.mnemonic.messaging.documentchannel.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.messaging.documentchannel.DocumentBatch;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import no.mnemonic.messaging.documentchannel.DocumentChannelSubscription;
import no.mnemonic.messaging.documentchannel.DocumentSource;

import javax.jms.BytesMessage;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;


/**
 * JMS version of a document channel source. The source is configured with a JMS session, which points to a JMS server/cluster
 * and a destination, which may be a topic or a queue.
 * <p>
 * Using multiple sources against a Queue will load-balance the incoming documents between the sources.
 * Using multiple sources against a Topic will provide individual copies of each incoming document to each source.
 *
 * @param <T> document type
 */
public class JMSDocumentSource<T> implements DocumentSource<T> {

  private static final Logger LOGGER = Logging.getLogger(JMSDocumentSource.class);
  private static final int MAX_RETRIES = 5;

  private final Supplier<JMSSession> sessionProvider;
  private final Class<T> type;
  private final AtomicReference<JMSSession> currentSession = new AtomicReference<>();
  private final AtomicBoolean sessionStarted = new AtomicBoolean();
  private final AtomicBoolean subscriberAttached = new AtomicBoolean();

  private final LongAdder sessionsCreated = new LongAdder();
  private final LongAdder connectionExceptions = new LongAdder();

  private JMSDocumentSource(Supplier<JMSSession> sessionProvider, Class<T> type) {
    this.sessionProvider = sessionProvider;
    this.type = type;
    if (!SetUtils.in(type, String.class, byte[].class)) {
      throw new IllegalArgumentException("Invalid type: " + type);
    }
  }

  @Override
  public DocumentChannelSubscription createDocumentSubscription(DocumentChannelListener<T> listener) {
    if (listener == null) throw new IllegalArgumentException("listener not set");
    if (currentSession.get() != null) throw new IllegalStateException("Subscriber already created");
    try {
      setupSubscription(listener);
      return this::closeSession;
    } catch (JMSDocumentChannelException | JMSException e) {
      throw new IllegalStateException("Unable to subscribe to channel", e);
    }
  }

  @Override
  public DocumentBatch<T> poll(Duration duration) {
    if (duration == null) throw new IllegalArgumentException("duration not set");
    if (subscriberAttached.get()) throw new IllegalStateException("This channel already has a subscriber");
    try {
      if (sessionStarted.compareAndSet(false, true)) {
        getSession().start();
      }
      Message msg = getSession().getConsumer().receive(duration.toMillis());
      return createBatch(msg);
    } catch (JMSException | JMSDocumentChannelException e) {
      throw new IllegalStateException("Unable to poll from session", e);
    }
  }

  @Override
  public void close() {
    closeSession();
  }

  //private methods

  private DocumentBatch<T> createBatch(Message msg) {
    List<T> result = new ArrayList<>();
    if (msg != null) {
      ifNotNullDo(handleMessage(msg), result::add);
    }
    return new DocumentBatch<T>() {
      @Override
      public Collection<T> getDocuments() {
        return result;
      }

      @Override
      public void acknowledge() {
        try {
          if (msg != null) msg.acknowledge();
        } catch (JMSException e) {
          throw new IllegalStateException("Error acknowledging document", e);
        }
      }

      @Override
      public void reject() {
        closeSession();
      }
    };
  }

  private void setupSubscription(DocumentChannelListener<T> listener) throws JMSDocumentChannelException, JMSException {
    JMSSession session = getSession();
    session.setExceptionListener(new RecoveringExceptionListener(listener));
    MessageConsumer consumer = session.getConsumer();
    consumer.setMessageListener(new ConvertingMessageListener(listener));
    session.start();
    sessionStarted.set(true);
    subscriberAttached.set(true);
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

  private T handleMessage(Message message) {
    if (message == null) return null;
    try {
      if (type.equals(byte[].class) && message instanceof BytesMessage) {
        BytesMessage msg = (BytesMessage) message;
        byte[] data = new byte[(int) msg.getBodyLength()];
        msg.readBytes(data);
        return (T)data;
      } else if (type.equals(String.class) && message instanceof TextMessage) {
        TextMessage msg = (TextMessage) message;
        return (T)msg.getText();
      } else {
        LOGGER.warning("Received unexpected message type %s (type=%s)", message.getClass(), type);
        return null;
      }
    } catch (JMSException e) {
      LOGGER.warning(e, "Error reading document");
      return null;
    }
  }

  private class ConvertingMessageListener implements MessageListener {
    private final DocumentChannelListener<T> listener;

    ConvertingMessageListener(DocumentChannelListener<T> listener) {
      this.listener = listener;
    }

    @Override
    public void onMessage(Message message) {
      T document = handleMessage(message);
      if (document != null) {
        listener.documentReceived(document);
      }
    }
  }

  private class RecoveringExceptionListener implements ExceptionListener {
    private final DocumentChannelListener<T> listener;

    RecoveringExceptionListener(DocumentChannelListener<T> listener) {
      this.listener = listener;
    }

    @Override
    public void onException(JMSException exception) {
      connectionExceptions.increment();
      LOGGER.warning("Received channel error, resetting session");

      for (int i = 0; i < MAX_RETRIES; i++) {
        try {
          //close JMS session on exception
          closeSession();
          //then retry
          setupSubscription(listener);
          //return when successful
          return;
        } catch (JMSDocumentChannelException | JMSException e) {
          LOGGER.error(e, "Unable to recover channel");
        }
      }
      LOGGER.error("Could not reconnect after %d retries, giving up", MAX_RETRIES);
    }
  }

  public long getSessionsCreated() {
    return sessionsCreated.longValue();
  }

  public long getConnectionExceptions() {
    return connectionExceptions.longValue();
  }

  //builders

  public static <T>Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {

    //fields
    private Supplier<JMSSession> sessionProvider;
    private Class<T> type;

    public JMSDocumentSource<T> build() {
      return new JMSDocumentSource<>(sessionProvider, type);
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
