# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.2.19] - 2019-09-15
### Fixed
ARGUS-14802 
- Add handling of XStream ForbiddenClassException.
- If deserialization error occurs on server side when receiving client signal,
the server will respond with an IllegalDeserializationException and close the channel.
- If deserialization error occurs on client side when receiving server response,
the client will forward the IllegalDeserializationException to the upstream client.
- In both cases, the client will be notified using `notifyError(Throwable)`
 

## [1.2.18] - 2019-09-13
### Changed
ARGUS-14395
- Upgrade kafka-clients from 0.10.2.2 to 2.1.1

## [1.2.17] - 2019-08-06
### Added
ARGUS-14331
 - Added support for registering custom serializers/deserializers for the Kafka 
producer/consumer providers, allowing the creation of custom typed document channels.

ARGUS-14332
 - Added convenience support for disabling the channel by configuration, allowing
simple implementation of a channel disable switch. 