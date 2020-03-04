package no.mnemonic.messaging.requestsink.jms.serializer;

import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.messaging.requestsink.Message;

import java.io.IOException;

public interface MessageSerializer extends MetricAspect {

  /**
   * @return a serializer identifier, to allow JMS receiver to distinguish between sender formats
   */
  String serializerID();

  /**
   * Serialize the message
   * @param msg message to serialize
   * @return message in serialized format
   * @throws IOException if serialization fails
   */
  byte[] serialize(Message msg) throws IOException;

  /**
   * Deserialize bytes into a message
   * @param msgbytes message bytes
   * @param classLoader classloader which knows any involved types
   * @param <T> expected message type
   * @return the deserialized message
   * @throws IOException if deserialization fails
   */
  <T extends Message> T deserialize(byte[] msgbytes, ClassLoader classLoader) throws IOException;
}
