package no.mnemonic.messaging.requestsink.jms.serializer;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.HierarchicalStreamDriver;
import com.thoughtworks.xstream.io.xml.Xpp3Driver;
import com.thoughtworks.xstream.security.ForbiddenClassException;
import com.thoughtworks.xstream.security.NoTypePermission;
import com.thoughtworks.xstream.security.NullPermission;
import com.thoughtworks.xstream.security.PrimitiveTypePermission;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.jms.ExceptionMessage;
import no.mnemonic.messaging.requestsink.jms.IllegalDeserializationException;

import javax.jms.JMSException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.collections.MapUtils.map;
import static no.mnemonic.commons.utilities.collections.SetUtils.addToSet;
import static no.mnemonic.commons.utilities.collections.SetUtils.set;
import static no.mnemonic.messaging.requestsink.jms.util.JMSUtils.assertNotNull;

public class XStreamMessageSerializer implements MessageSerializer {

  private static final Logger LOGGER = Logging.getLogger(XStreamMessageSerializer.class);
  private static final String DEFAULT_SERIALIZER_ID = "XSTREAM";
  private final XStream decodingXstream;
  private final XStream encodingXstream;
  private final HierarchicalStreamDriver driver;
  private final String serializerID;

  private final LongAdder serializeCount = new LongAdder();
  private final LongAdder serializeError = new LongAdder();
  private final LongAdder serializeTime = new LongAdder();
  private final LongAdder serializeMsgSize = new LongAdder();
  private final LongAdder deserializeCount = new LongAdder();
  private final LongAdder deserializeError = new LongAdder();
  private final LongAdder deserializeForbiddenClassError = new LongAdder();
  private final LongAdder deserializeTime = new LongAdder();
  private final LongAdder deserializeMsgSize = new LongAdder();

  /**
   * @param allowedClassesRegex list of allowed classes regex
   * @param packageAliases      map of alias->package
   * @param serializerID        ID of this serializer
   * @param decodingXstreamCustomizer
   * @param encodingXstreamCustomizer
   */
  private XStreamMessageSerializer(HierarchicalStreamDriver driver,
                                   Collection<Class> allowedClasses,
                                   Collection<String> allowedClassesRegex,
                                   Map<String, Class> typeAliases,
                                   Map<String, String> packageAliases,
                                   Map<String, Class> decodingTypeAliases,
                                   Map<String, String> decodingPackageAliases,
                                   String serializerID,
                                   Consumer<XStream> decodingXstreamCustomizer,
                                   Consumer<XStream> encodingXstreamCustomizer) {
    this.driver = assertNotNull(driver, "driver not set");
    this.serializerID = assertNotNull(serializerID, "serializerID not set");
    decodingXstream = new XStream(driver);
    encodingXstream = new XStream(driver);
    decodingXstream.addPermission(NoTypePermission.NONE);
    decodingXstream.addPermission(PrimitiveTypePermission.PRIMITIVES);
    decodingXstream.addPermission(NullPermission.NULL);
    decodingXstream.allowTypesByRegExp(list(allowedClassesRegex).toArray(new String[]{}));
    decodingXstream.allowTypes(list(allowedClasses).toArray(new Class[]{}));
    decodingXstream.ignoreUnknownElements();

    //allow types needed for proper error handling
    decodingXstream.allowTypes(new Class[]{
            JMSException.class,
            List.class,
            StackTraceElement.class,
            IllegalDeserializationException.class,
            ExceptionMessage.class
    });
    decodingXstream.allowTypesByRegExp(new String[]{"java.util.Collections\\$UnmodifiableList"});

    map(packageAliases).forEach((a, p) -> {
      decodingXstream.aliasPackage(a, p);
      encodingXstream.aliasPackage(a, p);
    });
    map(typeAliases).forEach((a, c) -> {
      decodingXstream.alias(a, c);
      encodingXstream.alias(a, c);
    });
    map(decodingPackageAliases).forEach(decodingXstream::aliasPackage);
    map(decodingTypeAliases).forEach(decodingXstream::alias);

    //allow non-standard customization
    if (decodingXstreamCustomizer != null) decodingXstreamCustomizer.accept(decodingXstream);
    if (encodingXstreamCustomizer != null) encodingXstreamCustomizer.accept(encodingXstream);
  }

  @Override
  public String serializerID() {
    return serializerID;
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
            .addData("deserializeForbiddenClassError", deserializeForbiddenClassError)
            .addData("deserializeTime", deserializeTime)
            .addData("deserializeMsgSize", deserializeMsgSize);
  }

  @Override
  public byte[] serialize(Message msg) throws IOException {
    serializeCount.increment();

    try (
            TimerContext ignored = TimerContext.timerMillis(serializeTime::add);
            ByteArrayOutputStream baos = new ByteArrayOutputStream()
    ) {
      encodingXstream.marshal(msg, driver.createWriter(baos));
      serializeMsgSize.add(baos.size());
      LOGGER.debug("XStream serialize driver=%s size=%d", driver.getClass(), baos.size());
      return baos.toByteArray();
    } catch (Exception e) {
      serializeError.increment();
      LOGGER.error(e, "Error in serialize");
      throw new IOException("Error in serialize", e);
    }
  }

  @Override
  public <T extends Message> T deserialize(byte[] msgbytes, ClassLoader classLoader) throws IOException {
    deserializeCount.increment();
    deserializeMsgSize.add(msgbytes.length);
    if (LOGGER.isDebug()) {
      LOGGER.debug("XStream deserialize driver=%s size=%d", driver.getClass(), msgbytes.length);
    }

    try (
            TimerContext ignored = TimerContext.timerMillis(deserializeTime::add);
            ByteArrayInputStream bais = new ByteArrayInputStream(msgbytes)
    ) {
      //noinspection unchecked
      return (T) decodingXstream.unmarshal(driver.createReader(bais));
    } catch (ForbiddenClassException e) {
      deserializeForbiddenClassError.increment();
      LOGGER.error(e, "Forbidden class in deserialize");
      throw new IllegalDeserializationException(e.getMessage());
    } catch (Throwable e) {
      if (e.getCause() instanceof ForbiddenClassException) {
        deserializeForbiddenClassError.increment();
        LOGGER.error(e, "Forbidden class in deserialize");
        throw new IllegalDeserializationException(e.getMessage());
      } else {
        deserializeError.increment();
        LOGGER.error(e, "Error in deserialize");
        throw new IOException("Error in deserialize", e);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private HierarchicalStreamDriver driver = new Xpp3Driver();
    private Set<String> allowedClassesRegex = set();
    private Set<Class> allowedClasses = set();
    private Map<String, String> packageAliases = map();
    private Map<String, Class> typeAliases = map();
    private Map<String, String> decodingPackageAliases = map();
    private Map<String, Class> decodingTypeAliases = map();
    private Consumer<XStream> decodingXstreamCustomizer;
    private Consumer<XStream> encodingXstreamCustomizer;
    private String serializerID = DEFAULT_SERIALIZER_ID;

    private Builder() {
    }

    //fields

    public XStreamMessageSerializer build() {
      return new XStreamMessageSerializer(driver,
              allowedClasses, allowedClassesRegex,
              typeAliases, packageAliases,
              decodingTypeAliases, decodingPackageAliases,
              serializerID, decodingXstreamCustomizer, encodingXstreamCustomizer);
    }

    //setters

    /**
     * Allow arbitrary non-standard customization of the decoding xstream converter
     * <b>WARNING</b> Use with care!
     * @param decodingXstreamCustomizer any code modifying the config of the decoding converter
     */
    public Builder setDecodingXstreamCustomizer(Consumer<XStream> decodingXstreamCustomizer) {
      this.decodingXstreamCustomizer = decodingXstreamCustomizer;
      return this;
    }

    /**
     * Allow arbitrary non-standard customization of the encoding xstream converter
     * <b>WARNING</b> Use with care!
     * @param encodingXstreamCustomizer any code modifying the config of the encoding converter
     */
    public Builder setEncodingXstreamCustomizer(Consumer<XStream> encodingXstreamCustomizer) {
      this.encodingXstreamCustomizer = encodingXstreamCustomizer;
      return this;
    }

    public Builder setSerializerID(String serializerID) {
      this.serializerID = serializerID;
      return this;
    }

    public Builder setDriver(HierarchicalStreamDriver driver) {
      this.driver = driver;
      return this;
    }

    public Builder addDecodingPackageAlias(String alias, String packageName) {
      this.decodingPackageAliases = MapUtils.addToMap(this.decodingPackageAliases, alias, packageName);
      return this;
    }

    public Builder addDecodingTypeAlias(String alias, Class type) {
      this.decodingTypeAliases = MapUtils.addToMap(this.decodingTypeAliases, alias, type);
      return this;
    }

    public Builder addPackageAlias(String alias, String packageName) {
      this.packageAliases = MapUtils.addToMap(this.packageAliases, alias, packageName);
      return this;
    }

    public Builder addTypeAlias(String alias, Class type) {
      this.typeAliases = MapUtils.addToMap(this.typeAliases, alias, type);
      return this;
    }

    public Builder addAllowedClass(String allowedClassRegex) {
      this.allowedClassesRegex = addToSet(this.allowedClassesRegex, allowedClassRegex);
      return this;
    }

    public Builder addAllowedClass(Class allowedClass) {
      this.allowedClasses = addToSet(this.allowedClasses, allowedClass);
      return this;
    }
  }


}
