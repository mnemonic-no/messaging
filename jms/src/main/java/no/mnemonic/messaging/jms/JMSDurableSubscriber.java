package no.mnemonic.messaging.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.api.TransactedObjectSource;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.naming.NamingException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;

public class JMSDurableSubscriber<T extends Serializable> extends JMSObjectSource<T> implements
        TransactedObjectSource<T> {

  private static final Logger LOGGER = Logging.getLogger(JMSDurableSubscriber.class);

  // properties
  private final String durableName;

  // variables
  private TopicSubscriber topicSubscriber;

  public JMSDurableSubscriber(List<JMSConnection> connections, String destinationName, boolean transacted, long failbackInterval,
                              int timeToLive, int priority, boolean persistent, boolean temporary,
                              Consumer<Runnable> executor, long maxReconnectTime, String durableName) {
    super(connections, destinationName, transacted, failbackInterval, timeToLive, priority, persistent, temporary, executor, maxReconnectTime);
    this.durableName = durableName;
  }

  // interface methods

  protected MessageConsumer getMessageConsumer() throws JMSException, NamingException {
    synchronized (this) {
      if (topicSubscriber == null) {
        topicSubscriber = getSession().createDurableSubscriber((Topic) getDestination(), durableName, null, false);
      }
      return topicSubscriber;
    }
  }

  @Override
  public void close() {
    try {
      if (topicSubscriber != null) {
        topicSubscriber.close();
      }
    } catch (Exception e) {
      LOGGER.error(e, "Error on close");
    } finally {
      super.close();
      topicSubscriber = null;
    }
  }

  @Override
  public void invalidate() {
    super.invalidate();
    topicSubscriber = null;
  }

}