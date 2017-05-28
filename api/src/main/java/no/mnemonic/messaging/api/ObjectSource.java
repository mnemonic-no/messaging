/*
 * ObjectSource.java:
 * Created on 09.aug.2004
 * (C) Copyright 2004 mnemonic as
 * All rights reserved.
 */
package no.mnemonic.messaging.api;

/**
 * An ObjectSource is an abstraction for the requestSource end of a pipe.
 *
 * Users may read objects from the pipe, but what kind of objects may come out of the pipe,
 * depends on the implementation of the object requestSource.
 */
public interface ObjectSource<T> {

  /**
   * This method blocks until the requestSource delivers an object, or until maxWait millis
   * have passed. Implementations MUST make sure that the requestSource delivers data within
   * maxWait millis, throws TimeOutException if no data is available in maxWait millis,
   * or returns null when the requestSource is closed. If the requestSource is closed at the time
   * of the method call, it is preferred that the requestSource returns null immediately.
   *
   * @param maxWait max waittime before returning. A value of null means wait forever.
   * @return the next available object from this objectsource, or null
   *         if no more objects are available. If no object is available
   *         within maxWait seconds, a TimeOutException is thrown.
   *         If the method returns null, there is no more objects to deliver.
   * @throws TimeOutException   if maxWait has elapsed without any objects to serve.
   * @throws MessagingException if a recoverable error occurs
   */
  T receiveObject(long maxWait) throws TimeOutException, MessagingException;
}