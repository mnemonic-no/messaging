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
import no.mnemonic.commons.utilities.collections.MapUtils;
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

  /**
   * @param allowedClassesRegex list of allowed classes regex
   * @param packageAliases      map of alias->package
   * @param serializerID        ID of this serializer
   */
  private XStreamMessageSerializer(HierarchicalStreamDriver driver,
                                   Collection<Class> allowedClasses,
                                   Collection<String> allowedClassesRegex,
                                   Map<String, Class> typeAliases,
                                   Map<String, String> packageAliases,
                                   Map<String, Class> decodingTypeAliases,
                                   Map<String, String> decodingPackageAliases,
                                   String serializerID) {
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
  }

  @Override
  public String serializerID() {
    return serializerID;
  }

  @Override
  public byte[] serialize(no.mnemonic.messaging.requestsink.Message msg) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      encodingXstream.marshal(msg, driver.createWriter(baos));
      LOGGER.debug("XStream serialize driver=%s size=%d", driver.getClass(), baos.size());
      return baos.toByteArray();
    } catch (Exception e) {
      LOGGER.error(e, "Error in serialize");
      throw new IOException("Error in serialize", e);
    }
  }

  @Override
  public <T extends no.mnemonic.messaging.requestsink.Message> T deserialize(byte[] msgbytes, ClassLoader classLoader) throws IOException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(msgbytes)) {
      LOGGER.debug("XStream deserialize driver=%s size=%d", driver.getClass(), msgbytes.length);
      //noinspection unchecked
      return (T) decodingXstream.unmarshal(driver.createReader(bais));
    } catch (ForbiddenClassException e) {
      LOGGER.error(e, "Forbidden class in deserialize");
      throw new IllegalDeserializationException(e.getMessage());
    } catch (Exception e) {
      if (e.getCause() instanceof ForbiddenClassException) {
        LOGGER.error(e, "Forbidden class in deserialize");
        throw new IllegalDeserializationException(e.getMessage());
      } else {
        LOGGER.error(e, "Error in deserialize");
        throw new IOException(e);
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
    private String serializerID = DEFAULT_SERIALIZER_ID;

    private Builder() {
    }

    //fields

    public XStreamMessageSerializer build() {
      return new XStreamMessageSerializer(driver,
              allowedClasses, allowedClassesRegex,
              typeAliases, packageAliases,
              decodingTypeAliases, decodingPackageAliases,
              serializerID);
    }

    //setters

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
