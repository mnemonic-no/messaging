package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import no.mnemonic.messaging.requestsink.jms.serializer.MessageSerializer;

import java.io.IOException;

public class JMSRequestSinkJavaSerializationTest extends AbstractJMSRequestSinkTest {

  private DefaultJavaMessageSerializer serializer = new DefaultJavaMessageSerializer();

  @Override
  protected MessageSerializer serializer() throws IOException {
    return serializer;
  }

}
