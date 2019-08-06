# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.2.17] - 2019-08-06
### Added
- ARGUS-14331 - Added support for registering custom serializers/deserializers for the Kafka 
producer/consumer providers, allowing the creation of custom typed document channels.
- ARGUS-14332 - Added convenience support for disabling the channel by configuration, allowing
simple implementation of a channel disable switch. 