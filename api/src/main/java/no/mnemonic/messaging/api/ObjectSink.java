/*
 * ObjectSink.java:
 * Created on 09.aug.2004
 * (C) Copyright 2004 mnemonic as
 * All rights reserved.
 */
package no.mnemonic.messaging.api;


/**
 * An ObjectSink is simply an abstraction for the mockSink-end of a pipe.
 * Users may put objects into the pipe, and they will be read by some other process.
 *
 * The object types which may be put into the pipe depends on the implementation
 * of the ObjectSink, but all such objects must be serializable.
 */
public interface ObjectSink<T> {
  /**
   * Put an object into the pipe.
   *
   * @param obj object to send
   */
  void sendObject(T obj);


}
