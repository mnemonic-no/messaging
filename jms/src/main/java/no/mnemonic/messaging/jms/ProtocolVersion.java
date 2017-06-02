package no.mnemonic.messaging.jms;

public enum ProtocolVersion {
  V13(JMSBase.PROTOCOL_VERSION_13),
  V16(JMSBase.PROTOCOL_VERSION_16);

  private String versionString;

  ProtocolVersion(String versionString) {
    this.versionString = versionString;
  }

  public String getVersionString() {
    return versionString;
  }
}
