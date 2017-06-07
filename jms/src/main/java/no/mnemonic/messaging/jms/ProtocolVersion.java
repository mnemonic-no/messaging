package no.mnemonic.messaging.jms;

public enum ProtocolVersion {
  V1(JMSBase.PROTOCOL_VERSION_1);

  private String versionString;

  ProtocolVersion(String versionString) {
    this.versionString = versionString;
  }

  public String getVersionString() {
    return versionString;
  }
}
