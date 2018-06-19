package no.mnemonic.messaging.requestsink.jms.serializer;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.Message;

import java.io.*;

import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.*;

public class DefaultJavaMessageSerializer implements MessageSerializer {

  private static final Logger LOGGER = Logging.getLogger(DefaultJavaMessageSerializer.class);
  private static final String SERIALIZER_ID = "JSER";

  @Override
  public String serializerID() {
    return SERIALIZER_ID;
  }

  @Override
  public byte[] serialize(Message msg) throws IOException {
    assertNotNull(msg, "Object not set");
    try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos)
    ) {
      oos.writeObject(msg);
      LOGGER.debug("JSER serialize size=%d", baos.size());
      return baos.toByteArray();
    }
  }

  @Override
  public <T extends Message> T deserialize(byte[] msgbytes, ClassLoader classLoader) throws IOException {
    assertNotNull(msgbytes, "Data not set");
    assertNotNull(classLoader, "ClassLoader not set");
    LOGGER.debug("JSER deserialize size=%d", msgbytes.length);
    try (ObjectInputStream ois = new ClassLoaderAwareObjectInputStream(new ByteArrayInputStream(msgbytes), classLoader)) {
      //noinspection unchecked
      return (T) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
}
