package no.mnemonic.messaging.requestsink.jms.serializer;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.messaging.requestsink.Message;

import java.io.*;
import java.util.concurrent.atomic.LongAdder;

import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.assertNotNull;

public class DefaultJavaMessageSerializer implements MessageSerializer {

  private static final Logger LOGGER = Logging.getLogger(DefaultJavaMessageSerializer.class);
  private static final String SERIALIZER_ID = "JSER";

  private final LongAdder serializeCount = new LongAdder();
  private final LongAdder serializeError = new LongAdder();
  private final LongAdder serializeTime = new LongAdder();
  private final LongAdder serializeMsgSize = new LongAdder();
  private final LongAdder deserializeCount = new LongAdder();
  private final LongAdder deserializeError = new LongAdder();
  private final LongAdder deserializeTime = new LongAdder();
  private final LongAdder deserializeMsgSize = new LongAdder();

  @Override
  public String serializerID() {
    return SERIALIZER_ID;
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    return new MetricsData()
            .addData("serializeCount", serializeCount)
            .addData("serializeError", serializeError)
            .addData("serializeTime", serializeTime)
            .addData("serializeMsgSize", serializeMsgSize)
            .addData("deserializeCount", deserializeCount)
            .addData("deserializeError", deserializeError)
            .addData("deserializeTime", deserializeTime)
            .addData("deserializeMsgSize", deserializeMsgSize);
  }

  @Override
  public byte[] serialize(Message msg) throws IOException {
    assertNotNull(msg, "Object not set");
    serializeCount.increment();

    try (
            TimerContext ignored = TimerContext.timerMillis(serializeTime::add);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos)
    ) {
      oos.writeObject(msg);
      serializeMsgSize.add(baos.size());
      LOGGER.debug("JSER serialize size=%d", baos.size());
      return baos.toByteArray();
    } catch (Exception e) {
      serializeError.increment();
      LOGGER.error(e, "Error in serialize");
      throw new IOException("Error in serialize", e);
    }
  }

  @Override
  public <T extends Message> T deserialize(byte[] msgbytes, ClassLoader classLoader) throws IOException {
    assertNotNull(msgbytes, "Data not set");
    assertNotNull(classLoader, "ClassLoader not set");
    deserializeCount.increment();
    deserializeMsgSize.add(msgbytes.length);
    LOGGER.debug("JSER deserialize size=%d", msgbytes.length);

    try (
            TimerContext ignored = TimerContext.timerMillis(deserializeTime::add);
            ObjectInputStream ois = new ClassLoaderAwareObjectInputStream(new ByteArrayInputStream(msgbytes), classLoader)
    ) {
      //noinspection unchecked
      return (T) ois.readObject();
    } catch (Exception e) {
      deserializeError.increment();
      LOGGER.error(e, "Error in deserialize");
      throw new IOException("Error in deserialize", e);
    }
  }
}
