package no.mnemonic.messaging.requestsink.jms;

import javax.jms.JMSException;
import java.util.Objects;

public enum ProtocolVersion {

  V3(3),
  V4(4);

  private final int version;

  ProtocolVersion(int version) {
    this.version = version;
  }

  public String getVersionString() {
    return String.valueOf(version);
  }

  public boolean atLeast(ProtocolVersion v) {
    return v != null && version >= v.version;
  }

  public static ProtocolVersion versionOf(String protostr) throws JMSException {
    for (ProtocolVersion v : values()) {
      if (Objects.equals(v.getVersionString(), protostr)) return v;
    }
    throw new JMSException("Invalid protocol version: " + protostr);
  }


}
