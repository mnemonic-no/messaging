/*
 * JMSObjectSource.java:
 * Created on 10.aug.2004
 * (C) Copyright 2004 mnemonic as
 * All rights reserved.
 */
package no.mnemonic.messaging.jms;


import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.api.*;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.naming.NamingException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class JMSObjectSource<T extends Serializable> extends JMSBase
        implements TransactedObjectSource<T>, ObjectSource<T>, AsynchronousObjectSource<T>, MessageListener, ExceptionListener, LifecycleAspect {

  private static final Logger LOGGER = Logging.getLogger(JMSObjectSource.class);


  private long maxReconnectTime = 60 * 60 * 1000; //1 hour default

  private final Set<ObjectListener<T>> listeners = Collections.synchronizedSet(new HashSet<>());
  private final Set<ErrorListener> errorListeners = Collections.synchronizedSet(new HashSet<>());

  public JMSObjectSource(List<JMSConnection> connections, String destinationName, boolean transacted, long failbackInterval, int timeToLive, int priority, boolean persistent, boolean temporary, Consumer<Runnable> executor, long maxReconnectTime) {
    super(connections, destinationName, transacted, failbackInterval, timeToLive, priority, persistent, temporary, executor);
    this.maxReconnectTime = maxReconnectTime;
  }

  @Override
  public void startComponent() {
    super.startComponent();
    if (isAsyncMode()) reconnectInSeparateThread();
  }

  public void addObjectListener(ObjectListener<T> listener) {
    if (listeners.isEmpty()) reconnectInSeparateThread();
    listeners.add(listener);
  }

  public void removeObjectListener(ObjectListener<T> listener) {
    if (!listeners.remove(listener)) return;
    if (listeners.isEmpty()) {
      try {
        getConnection().deregister(this);
      } catch (JMSException | NamingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void addErrorListener(ErrorListener listener) {
    errorListeners.add(listener);
  }

  @Override
  public void removeErrorListener(ErrorListener listener) {
    errorListeners.remove(listener);
  }

  public void onException(JMSException e) {
    //do invalidate first
    invalidate();
    //then notify error to listeners
    errorListeners.forEach(l -> LambdaUtils.tryTo(() -> l.notifyError(e)));
  }

  public void onMessage(Message message) {
    listeners.forEach(l -> LambdaUtils.tryTo(
            () -> l.receiveObject(JMSUtils.extractObject(message)),
            ex -> LOGGER.error(ex, "Error in onMessage"))
    );
  }

  @SuppressWarnings("unchecked")
  public T receiveObject(long maxWait) throws TimeOutException {
    if (isClosed()) throw new RuntimeException("closed");
    Message msg = receiveMessage(maxWait);
    Serializable result;
    try {
      result = JMSUtils.extractObject(msg);
    } catch (JMSException e) {
      LOGGER.error(e, "Error in receiveObject, msg=%s", msg.toString());
      invalidate();
      throw new MessagingException(e);
    }
    return (T) result;
  }


  @SuppressWarnings({"UnusedDeclaration"})
  public Transaction requestSourceTransaction() {
    return new Transaction() {
      public void commit() {
        if (!isTransacted())
          return;
        try {
          getSession().commit();
        } catch (Exception e) {
          invalidate();
          throw new MessagingException(e);
        }
      }

      public void rollback() {
        if (!isTransacted())
          return;
        try {
          getSession().rollback();
        } catch (Exception e) {
          invalidate();
          throw new MessagingException(e);
        }
      }
    };
  }


  @Override
  public void invalidate() {
    super.invalidate();
    if (isAsyncMode()) {
      reconnectInSeparateThread();
    }
  }

  //private methods
  private boolean isAsyncMode() {
    return !listeners.isEmpty();
  }

  /**
   * Receives a JMS message from the consumer
   *
   * @param maxWait max number of millis to wait for message
   * @return first message received within maxWait millis
   * @throws TimeOutException if no message was received within maxWait millis
   */
  private Message receiveMessage(final long maxWait) throws TimeOutException {
    try {
      long timeout = System.currentTimeMillis() + maxWait;

      long wait = timeout - System.currentTimeMillis();
      if (wait <= 0) {
        throw new TimeOutException();
      }
      //receive message
      Message msg = getMessageConsumer().receive(wait);
      //if timed out, throw exception
      if (msg == null)
        throw new TimeOutException();
      //discard message if it's an incompatible version
      if (!JMSUtils.isCompatible(msg)) {
        LOGGER.warning("Ignoring incompatible of incompatible version: %s", msg);
      }
      return msg;
    } catch (JMSException | NamingException e) {
      throw handleMessagingException(e);
    }
  }

  private void reconnectInSeparateThread() {
    if (isReconnecting()) return;
    //start reconnect job in separate thread
    submit(() -> doReconnect(maxReconnectTime, this, this));
  }

}