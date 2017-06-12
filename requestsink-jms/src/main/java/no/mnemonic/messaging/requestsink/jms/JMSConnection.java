package no.mnemonic.messaging.requestsink.jms;

import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.NamingException;

/**
 * Interface for a JMS connection
 *
 * @author joakim
 */
public interface JMSConnection {

  /**
   * Lookup JMS destination from this connection
   *
   * @param destinationName name of destination
   * @return the JMS destination object for this object
   * @throws JMSException    if an error occurs connecting to JMS
   * @throws NamingException if an error occurs looking up this destination
   */
  Destination lookupDestination(String destinationName) throws JMSException, NamingException;

  /**
   * Fetch a JMS session from this connection
   *
   * @param client     the requesting client
   * @param transacted true if the session should be transacted
   * @return a JMS session for this connection
   * @throws JMSException    if an error occurs connecting to JMS
   * @throws NamingException if an error occurs in the naming context
   */
  Session getSession(JMSBase client, boolean transacted) throws JMSException, NamingException;

  /**
   * Invalidate this connection, closing all sessions/resources
   */
  void invalidate();

  /**
   * Close this connection, invalidating all sessions/resources
   */
  void close();

  /**
   * @return true if this connection is closed
   */
  boolean isClosed();

  /**
   * Allow a JMS client to deregister from this connection
   *
   * @param client client which should have closed its session
   */
  void deregister(JMSBase client);

  /**
   * Allow a JMS client to register with this connection
   *
   * @param client client to register with this connection
   */
  void register(JMSBase client);

  /**
   * Register an exception listener to receive exception notifications
   *
   * @param listener the listener to register
   */
  void addExceptionListener(ExceptionListener listener);

  /**
   * Unregister an exception listener from this connection
   *
   * @param listener the listener to unregister
   */
  void removeExceptionListener(ExceptionListener listener);
}
