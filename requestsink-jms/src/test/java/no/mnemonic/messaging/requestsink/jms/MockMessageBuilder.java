package no.mnemonic.messaging.requestsink.jms;

import org.mockito.Mockito;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

class MockMessageBuilder<T extends Message> {

  private T msg;

  MockMessageBuilder(Class<T> type) {
    msg = Mockito.mock(type);
  }

  T build() {
    return msg;
  }

  MockMessageBuilder<T> withData(byte[] data) throws JMSException {
    when(((BytesMessage)msg).getBodyLength()).thenReturn((long)data.length);
    doAnswer(i->{
      //noinspection SuspiciousSystemArraycopy
      System.arraycopy(data, 0, i.getArgument(0), 0, data.length);
      return null;
    }).when((BytesMessage)msg).readBytes(any());
    return this;
  }

  MockMessageBuilder<T> withCorrelationID(String id) throws JMSException {
    when(msg.getJMSCorrelationID()).thenReturn(id);
    return this;
  }

  MockMessageBuilder<T> withProperty(String property, String value) throws JMSException {
    when(msg.propertyExists(property)).thenReturn(true);
    when(msg.getStringProperty(property)).thenReturn(value);
    return this;
  }

  MockMessageBuilder<T> withProperty(String property, int value) throws JMSException {
    when(msg.propertyExists(property)).thenReturn(true);
    when(msg.getIntProperty(property)).thenReturn(value);
    return this;
  }
  MockMessageBuilder<T> withProperty(String property, long value) throws JMSException {
    when(msg.propertyExists(property)).thenReturn(true);
    when(msg.getLongProperty(property)).thenReturn(value);
    return this;
  }
}
