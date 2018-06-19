package no.mnemonic.messaging.requestsink.jms;

import java.io.*;

import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.assertNotNull;

public class TestUtils {

  public static byte[] serialize(Serializable object) throws IOException {
    assertNotNull(object, "Object not set");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(object);
    oos.close();
    return baos.toByteArray();
  }

  public static <T extends Serializable> T unserialize(byte[] data) throws IOException, ClassNotFoundException {
    assertNotNull(data, "Data not set");
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
    //noinspection unchecked
    return (T) ois.readObject();
  }

}
