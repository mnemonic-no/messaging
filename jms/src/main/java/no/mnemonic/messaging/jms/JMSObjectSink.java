/*
 * JMSObjectSource.java:
 * Created on 10.aug.2004
 * (C) Copyright 2004 mnemonic as
 * All rights reserved.
 */
package no.mnemonic.messaging.jms;

import no.mnemonic.messaging.api.MessagingException;
import no.mnemonic.messaging.api.TransactedObjectSink;
import no.mnemonic.messaging.api.Transaction;

import javax.jms.Message;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Executors;

public class JMSObjectSink<T extends Serializable> extends JMSBase
        implements TransactedObjectSink<T> {

  private final JMSUtils.ProtocolVersion protocolVersion = JMSUtils.ProtocolVersion.V13;

  public JMSObjectSink(List<JMSConnection> connections, String destinationName, boolean transacted, long failbackInterval,
                       int timeToLive, int priority, boolean persistent, boolean temporary) {
    super(connections, destinationName, transacted, failbackInterval,
            timeToLive, priority, persistent, temporary,
            //only need for executor is to invalidate in separate thread
            Executors.newSingleThreadExecutor()
    );
  }

  public void sendObject(T obj) {
    if (isClosed()) throw new RuntimeException("closed");
    try {
      //encode and send in main thread
      //noinspection deprecation
      Message msg = protocolVersion == JMSUtils.ProtocolVersion.V16 ?
              JMSUtils.createByteMessage(getSession(), JMSUtils.serialize(obj)) :
              JMSUtils.createObjectMessage(getSession(), obj);
      sendJMSMessage(msg);
    } catch (Exception e) {
      throw handleMessagingException(e);
    }
  }

  @Override
  protected void sendJMSMessage(Message msg) {
    super.sendJMSMessage(msg);
    //untransacted JMS object sinks should check regularly for failback to primary connection
    if (!isTransacted())
      checkForFailBack();
  }

  @SuppressWarnings({"UnusedDeclaration"})
  public Transaction requestSinkTransaction() {
    return new Transaction() {

      public void commit() {
        if (!isTransacted()) return;
        try {
          //commit
          getSession().commit();
          //now would be a good time to fallback to primary connection, if needed
          checkForFailBack();
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
          checkForFailBack();
        } catch (Exception e) {
          invalidate();
          throw new MessagingException(e);
        }
      }
    };
  }
}
