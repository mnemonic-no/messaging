package no.mnemonic.messaging.requestsink.jms;

public enum ProtocolVersion {

  V1("1");

  private final String versionString;

  ProtocolVersion(String versionString) {
    this.versionString = versionString;
  }

  public String getVersionString() {
    return versionString;
  }
}
