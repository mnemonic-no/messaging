package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.messaging.requestsink.jms.serializer.MessageSerializer;
import no.mnemonic.messaging.requestsink.jms.serializer.XStreamMessageSerializer;

import java.io.IOException;

public class JMSRequestSinkXStreamTest extends AbstractJMSRequestSinkTest {

  private MessageSerializer serializer = XStreamMessageSerializer.builder()
          .addAllowedClass("no.mnemonic.*")
          .build();

  @Override
  protected MessageSerializer serializer() throws IOException {
    return serializer;
  }


}
