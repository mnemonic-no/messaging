# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.3.0] - 2021-03-09

### Added
ARGUS-22135
- Implemented cursor support in `KafkaDocumentSource`, allowing a consumer to receive a cursor to resume previous position.
    - New method `KafkaDocumentSource.seek()` to support seeking to a specified position 
    - New transport class `KafkaDocument` to carry a document with metadata
    - New method `KafkaDocumentSource.pollDocuments` returning `KafkaDocument` 
    - New interface `KafkaDocumentChannelListener` extending `DocumentChannelListener`, receiving `KafkaDocument` 

See [README.md](documentchannel-kafka/README.md) for details on how to use.

### Changed
ARGUS-22135
- Upgrade mnemonic-commons from 0.4.2 to 0.4.3
- Upgrade ActiveMQ from 5.16.0 to 5.16.1.
- Upgrade Kafka Clients from 2.1.1 to 2.7.0.
- Upgrade JUnit from 4.13.1 to 4.13.2.
- Upgrade Mockito from 3.6.28 to 3.8.0.
- Deprecated `DocumentSource.poll(long, TimeUnit)`, introducing `DocumentSource.poll(Duration)`
- Refactored `KafkaDocumentSource`, extracting KafkaConsumerWorker to separate class
- Added JUnit Jupiter framework, replacing legacy JUnit4 with Jupiter Vintage

## [1.2.28] - 2020-12-16
### Changed
ARGUS-20893
- Upgrade mnemonic-commons from 0.4.0 to 0.4.2.
- Upgrade ActiveMQ from 5.15.12 to 5.16.0.
- Upgrade XStream from 1.4.12 to 1.4.15.
- Upgrade JUnit from 4.13 to 4.13.1.
- Upgrade Mockito from 3.3.3 to 3.6.28.

## [1.2.27] - 2020-05-14
### Added
ARGUS-18043
- Added metrics of running/pending requests to JMSRequestProxy.

### Changed
ARGUS-18043
- Upgrade mnemonic-commons from 0.3.4 to 0.4.0.
- Upgrade ActiveMQ from 5.15.11 to 5.15.12.
- Upgrade XStream from 1.4.11.1 to 1.4.12.
- Upgrade Mockito from 3.3.0 to 3.3.3.
- Upgrade docker-compose-rule-junit4 from 1.4.2 to 1.5.0.

## [1.2.26] - 2020-04-30
### Fixed
ARGUS-18031
- Increased keepalive from 1s to 10s on fragmented responses in order to avoid timeouts due to slow connections.

## [1.2.25] - 2020-04-06
### Added
ARGUS-17790
- Created NOOP implementation of DocumentChannel.

## [1.2.24] - 2020-03-30
### Changed
ARGUS-17693
- Improved set-up and handling of serializers in JMSRequestProxy in order to report metrics generated by legacy serializer with protocol < V3.

## [1.2.23] - 2020-03-04
### Added
ARGUS-17115
- Added metrics to MessageSerializer implementations.

### Changed
ARGUS-17115
- Upgrade mnemonic-commons from 0.3.3 to 0.3.4.
- Upgrade Mockito from 3.2.4 to 3.3.0.

## [1.2.22] - 2020-02-17
### Changed
ARGUS-17074
- Upgrade ActiveMQ from 5.15.10 to 5.15.11.
- Upgrade JUnit from 4.12 to 4.13.
- Upgrade Mockito from 3.0.0 to 3.2.4.
- Upgrade docker-compose-rule-junit4 from 1.3.0 to 1.4.2.

## [1.2.21] - 2020-01-06
### Changed
ARGUS-16366
- Expose methods `setEncodingXstreamCustomizer(XStream)` and `setDecodingXstreamCustomizer(XStream)`
in XStreamMessageSerializer to allow advanced use cases needing access to arbitrary XStream
configuration options. 

## [1.2.20] - 2019-09-16
### Changed
ARGUS-14845
- Upgrade mnemonic-commons from 0.2.21 to 0.3.3.
- Upgrade ActiveMQ from 5.15.9 to 5.15.10.
- Upgrade Mockito from 2.27.0 to 3.0.0.
- Upgrade docker-compose-rule-junit4 from 0.35.0 to 1.3.0.

### Fixed
ARGUS-14845
- Correctly handle InterruptedException in RequestHandler.

## [1.2.19] - 2019-09-15
### Fixed
ARGUS-14802 
- Add handling of XStream ForbiddenClassException.
- If deserialization error occurs on server side when receiving client signal,
the server will respond with an IllegalDeserializationException and close the channel.
- If deserialization error occurs on client side when receiving server response,
the client will forward the IllegalDeserializationException to the upstream client.
- In both cases, the client will be notified using `notifyError(Throwable)`.

## [1.2.18] - 2019-09-13
### Changed
ARGUS-14395
- Upgrade kafka-clients from 0.10.2.2 to 2.1.1.

## [1.2.17] - 2019-08-06
### Added
ARGUS-14331
- Added support for registering custom serializers/deserializers for the Kafka 
producer/consumer providers, allowing the creation of custom typed document channels.

ARGUS-14332
- Added convenience support for disabling the channel by configuration, allowing
simple implementation of a channel disable switch. 