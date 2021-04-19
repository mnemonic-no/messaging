package no.mnemonic.messaging.requestsink.jms;

import org.mockito.Mockito;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

public class MockMessageBuilder<T extends Message> {

  private T msg;

  public MockMessageBuilder(Class<T> type) {
    msg = Mockito.mock(type);
  }

  public T build() {
    return msg;
  }

  public MockMessageBuilder<T> withData(byte[] data) throws JMSException {
    lenient().when(((BytesMessage) msg).getBodyLength()).thenReturn((long) data.length);
    lenient().doAnswer(i -> {
      //noinspection SuspiciousSystemArraycopy
      System.arraycopy(data, 0, i.getArgument(0), 0, data.length);
      return null;
    }).when((BytesMessage) msg).readBytes(any());
    return this;
  }

  public MockMessageBuilder<T> withCorrelationID(String id) throws JMSException {
    lenient().when(msg.getJMSCorrelationID()).thenReturn(id);
    return this;
  }

  public MockMessageBuilder<T> withProperty(String property, String value) throws JMSException {
    lenient().when(msg.propertyExists(property)).thenReturn(true);
    lenient().when(msg.getStringProperty(property)).thenReturn(value);
    return this;
  }

  public MockMessageBuilder<T> withProperty(String property, int value) throws JMSException {
    lenient().when(msg.propertyExists(property)).thenReturn(true);
    lenient().when(msg.getIntProperty(property)).thenReturn(value);
    return this;
  }

  public MockMessageBuilder<T> withProperty(String property, long value) throws JMSException {
    lenient().when(msg.propertyExists(property)).thenReturn(true);
    lenient().when(msg.getLongProperty(property)).thenReturn(value);
    return this;
  }
}
