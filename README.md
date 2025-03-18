mnemonic messaging
================

*mnemonic messaging* provides a set of reusable Java components for creating networked applications. Following the Don't-Repeat-Yourself principle those components contain common functionality which we have written over the years for our projects. We publish them as Open Source with the hope that they might be useful to others as well.

## Usage

[![Javadocs](https://javadoc.io/badge/no.mnemonic.messaging/documentchannel.svg?color=orange&label=documentchannel)](https://javadoc.io/doc/no.mnemonic.messaging/documentchannel)
[![Javadocs](https://javadoc.io/badge/no.mnemonic.messaging/documentchannel-kafka.svg?color=orange&label=documentchannel-kafka)](https://javadoc.io/doc/no.mnemonic.messaging/documentchannel-kafka)

See [RequestSink documentation](docs/REQUESTSINK.md) for more details on the RequestSink mechanism.

## Installation

All libraries provided by *mnemonic messaging* are directly available from Maven Central. Just declare a dependency in your pom.xml and start using it:

```xml
<dependency>
  <groupId>no.mnemonic.messaging</groupId>
  <artifactId>${artifactId}</artifactId>
  <version>${version}</version>
</dependency>
```

Replace ${artifactId} and ${version} with the library and version you want to use.

## Requirements

None, dependencies will be handled by Maven automatically.

## Known issues

See [Issues](https://github.com/mnemonic-no/messaging/issues).

## Contributing

See the [CONTRIBUTING.md](CONTRIBUTING.md) file.

## License

*mnemonic messaging* is released under the ISC License. See the bundled LICENSE file for details.