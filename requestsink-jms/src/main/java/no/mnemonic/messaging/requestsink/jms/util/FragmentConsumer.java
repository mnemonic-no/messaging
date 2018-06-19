package no.mnemonic.messaging.requestsink.jms.util;

import javax.jms.JMSException;
import java.io.IOException;

public interface FragmentConsumer {

  void fragment(byte[] data, int idx) throws JMSException, IOException;

  void end(int fragments, byte[] digest) throws JMSException;

}
