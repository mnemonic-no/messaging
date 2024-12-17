# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.4.12] - 2024-12-16
### Changed
ARGUS-48373
- Add configurable option `createIfMissing` in `KafkaDocumentSource` (default to false).
- This option is useful for api tests where kafka topics are not already created, to avoid timeouts on missing topics.

## [1.4.11] - 2024-11-04
### Changed
ARGUS-47033
- Upgraded dependencies 

## [1.4.10] - 2024-03-13
### Changed
ARGUSUSER-7231
- Add error logging in `KafkaDocumentDestination.sendDocument(T)` as there is no callback to register the error.  

## [1.4.0] - 2023-09-27
### Changed
ARGUSUSER-6549
- Protocol versions V1 and V2 are removed. New default protocol version is V3. 
- New SMB protocol version V4 introduced with flow control support.

### Upgrade notes
- The protocol version V4 introduces a flow control mechanism, where the client sends acknowledgements
back to the server to allow more responses, to avoid overrunning the client with a large resultset.
- The flow control introduces a "segment window size" on the server side, which is acquired from for every reply sent, 
and replenished by client acknowledgement messages. The client side is explicitly invoking `ResponseListener.responseAccepted()` to
signal to the server-side that a response has been processed, opening up the segment window for more data. This is done automatically by the
`RequestHandler` when response data is retrieved from the `RequestHandler`.
- Clients directly implementing their own `RequestContext` MUST invoke `ResponseListener.responseAccepted()` to avoid starvation.
- The client must not use V4 protocol version until the server side is upgraded to support V4.
- Client implementations may override the method `Message.getResponseWindowSize()` to control the window size per request. The
default window size is 50 messages.


## [1.3.33] - 2023-04-27
### Changed
ARGUS-35360
- Fixed typo in metric name in `JMSRequestProxy`

## [1.3.32] - 2023-04-19
### Changed
ARGUS-35360
- Changed `JMSRequestProxy` to set up one JMS consumer per priority (`standard`, `bulk` and `expedite`),
  using JMS filters to route the different priorities to separate executor thread pools.
- Any spare bulk capacity will always be available to standard or expedite clients.
- Any spare standard capacity will be available expedite clients. Bulk clients can use 
  spare capacity as long as there are a minimum number of unused threads.

### Upgrade notes
- The change in `JMSRequestProxy` should be transparent to clients,
  but beware that there are now new and separate configuration options 
  for the number of threads for each priority level, which clients should configure.
- Builder method `setMaxConcurrentCalls(int)` is deprecated.
- Use `setMaxConcurrentCallsStandard(int)`, `setMaxConcurrentCallsBulk(int)` and `setMaxConcurrentCallsExpedite(int)` instead.
- Method `setMinimumStandardCapacity(int)` configures how many spare threads must be available for bulk clients to be allowed to use standard capacity.   

## [1.3.31] - 2023-03-31
### Fixed
ARGUS-35324
- `NullDocumentSource.poll(Duration)` will now block the thread for the provided duration in order to prevent underlying threads from consuming CPU needlessly

## [1.3.30] - 2023-03-11
### Fixed
ARGUS-34837
- Fix that `KafkaDocumentSource` document subscription closes the document source when subscription is cancelled. 

## [1.3.29] - 2023-03-03
### Fixed
ARGUS-34571
- Ensure that `KafkaDocumentSource` fails with `KafkaInvalidSeekException` when invoking `seek()` with a cursor referencing non-existent partitions.

## [1.3.28] - 2023-03-03
### Added
ARGUS-33800
- Added SASL authentication support to `KafkaConsumerProvider` and `KafkaProducerProvider`

## [1.3.27] - 2023-03-03
### Fixed
ARGUS-33367
- Upgraded nexus-staging-maven-plugin in order to fix deployment to Maven Central.

## [1.3.26] - 2023-02-27
### Changed
ARGUS-32475
- Made project build with JDK17. **The project requires at least JDK11 to build now, but the code still executes on JDK8.**
- Upgraded dependencies to the newest versions.

## [1.3.25] - 2023-01-30
### Changed
ARGUS-33796
- Avoid closing `RequestHandler` on InterruptedException, to allow clients to deal with interrupt, possibly issuing an abort() before closing.

## [1.3.24] - 2023-01-30
### Changed
ARGUS-33796
- Renamed `JMSRequestSink/JMSRequestProxy` parameter "destinationName" to "queueName"
- Added optional `JMSRequestSink/JMSRequestProxy` parameter "topicName" to support broadcast topics
- Moved handling of abort signal to broadcast topic, to ensure that abort messages are processed on all nodes

### Upgrade notes
- Clients using `JMSRequestSink/JMSRequestProxy` should use `setQueueName()` instead of `setDestinationName()`, 
  and use `setTopicName()` to configure a broadcast topic.
- Until "topicName" is set, the `abort()` feature will not work 

## [1.3.23] - 2023-01-05
### Fixed
ARGUS-32838
- Fix instability in RequestSink protocol due to abort being executed on regular close.

## [1.3.22] - 2023-01-04
### Changed
ARGUS-32838
- Ensure that `JMSRequestSink.RequestListener` sends `abort()` when listener receives `close()`

## [1.3.21] - 2023-01-04
### Changed
ARGUS-32838
- Added method `RequestSink.abort(String)` to allow client to abort ongoing call.
- Add support for aborting ongoing client request in requestsink protocol.

## [1.3.20] - 2022-11-02
### Changed
ARGUS-31801
- Removed use of `Thread.interrupt()` when `RequestHandler` is interrupted.
- If interrupted, the `RequestHandler` will throw unchecked `MessagingInterruptedException` instead of `IllegalStateException`.

## [1.3.19] - 2022-10-14
### Changed
ARGUS-31386
- Reduced the default value for max poll records in KafkaConsumerProvider.

## [1.3.18] - 2022-09-15
### Fixed
INCIDENT-156015
- The `alive` metric in `KafkaDocumentSource` is no longer registered if no subscriber is attached

### Upgrade notes
- Nagios alarms will start triggering because this metric is no longer reporting any data. If this is true for your service, this is expected behavior.
  The way to solve this is to delete the metric from Graphite.

## [1.3.17] - 2022-09-05
### Fixed
ARGUS-30461
- Made KafkaDocumentBatch into public class

## [1.3.16] - 2022-08-26
### Changed
ARGUS-30461
- Added method `reject()` to the DocumentChannel `DocumentBatch` interface, allowing client to explicitly reject (rollback) the batch.
- Simplified Kafka implementation of Document Subscription.

### Upgrade notes
- Implementations of `KafkaDocumentChannelListener` are now *required* to implement 
  `void documentReceived(KafkaDocument<T> document)`. The default implementation of `void documentReceived(T document)` 
  is to throw `UnsupportedOperationException`. This may be overridden if the implementation
  can work without receiving a `KafkaDocument`

## [1.3.15] - 2022-08-22
### Added
ARGUS-30416
- Add support for custom key serializer in `KafkaDocumentDestination`.
- Clients may pass a lambda `Function<T, String>` to select the correct String value to partition the document by.
- Clients may use this to ensure that documents are partitioned consistently by some custom value.  

## [1.3.14] - 2021-12-06
### Changed
ARGUS-26277
- Add new method `KafkaDocumentSource.waitForAssignment(Duration)` to let clients wait for assignments to complete
- Improve `getCursor()` handling to make sure the cursor value is set to the current position of the source before returning 

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