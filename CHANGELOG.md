# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.9.4] - 2022-01-26

### Added in 1.9.4

 - Updated base image to address vulnerabilities

## [1.9.3] - 2021-12-13

### Fixed in 1.9.3

 - [Issue 253](https://github.com/Senzing/stream-loader/issues/253)

## [1.9.2] - 2021-11-04

### Added in 1.9.2

- trimmed image size

## [1.9.1] - 2021-10-11

### Added in 1.9.1

- Updated to senzing/senzing-base:1.6.2

## [1.9.0] - 2021-09-15

### Added in 1.9.0

- Added subcommands for Azure Queue and Azure SQL Database:
  - `azure-queue`
  - `azure-queue-withinfo`

## [1.8.4] - 2021-08-12

### Added in 1.8.4

- Create a `Dockerfile-with-data` which embeds `/opt/senzing/data`

## [1.8.3] - 2021-07-23

### Added in 1.8.3

- Added performace timing for G2 load

## [1.8.2] - 2021-07-15

### Added in 1.8.2

- Updated to senzing/senzing-base:1.6.1

## [1.8.1] - 2021-07-13

### Added in 1.8.1

- Updated to senzing/senzing-base:1.6.0

## [1.8.0] - 2021-06-30

### Added in 1.8.0

- [Directives](README.md#directives)

## [1.7.6] - 2021-05-26

### Changed in 1.7.6

- SQS based loads now only remove the record from the queue if it was successfully loaded to Senzing, permitting failed records to go to the dead letter queue, if configured.
- RabbitMQ virtual host is now a settable parameter.

## [1.7.5] - 2021-04-05

### Changed in 1.7.5

- Properly handle long running records that take a while to load (> 1 minute) to prevent RabbitMQ heartbeat timeouts.

## [1.7.4] - 2021-03-29

### Changed in 1.7.4

- Add logging messages around `g2_engine.initV2` and `g2_engine.primeEngine()`

## [1.7.3] - 2021-03-26

### Added in 1.7.3

- Additional debug logging for performance issues
- Support for `SENZING_SKIP_GOVERNOR` and `SENZING_SKIP_INFO_FILTER`

### Changed in 1.7.3

- Updated Dockerfile base to senzing/senzing-base:1.5.5

## [1.7.2] - 2021-02-22

### Added in 1.7.2

- Support for `SENZING_SQS_INFO_QUEUE_DELAY_SECONDS`

## [1.7.1] - 2021-02-18

### Added in 1.7.1

- Added `endpoint_url` in AWS SQS configuration.

## [1.7.0] - 2021-01-19

### Added in 1.7.0

- RabbitMQ, Kafka, and SQS loading subcommands (including withinfo variations) now support multiple records per message when formated as a json array.

## [1.6.5] - 2020-11-05

### Added in 1.6.5

- Support for random delay on SQS empty queue

## [1.6.4] - 2020-10-07

### Added in 1.6.4

- Support for `SENZING_DELAY_RANDOMIZED`
- Fixes in Governor support

## [1.6.3] - 2020-09-25

### Added in 1.6.3

- Support for stack traces using `gdb`.
- Support for `SENZING_RABBITMQ_HEARTBEAT_IN_SECONDS`

## [1.6.2] - 2020-09-21

### Changed in 1.6.2

- Add `SENZING_PRIME_ENGINE`.

## [1.6.1] - 2020-09-11

### Changed in 1.6.1

- Improved failure handling

## [1.6.0] - 2020-08-29

### Added in 1.6.0

- Support for Senzing Governor
- Support for RabbitMQ exchanges

### Removed in 1.6.0

- Deprecated support for multiple Kafka and RabbitMQ processes (i.e. not threads)

## [1.5.6] - 2020-08-06

### Changed in 1.5.6

- Fix issue with empty `RECORD_ID`
- Add support for `SENZING_SKIP_DATABASE_PERFORMANCE_TEST`

## [1.5.5] - 2020-07-23

### Changed in 1.5.5

- Upgrade to senzing/senzing-base:1.5.2

## [1.5.4] - 2020-02-24

### Changed in 1.5.4

- Updated Dockerfile base to senzing/senzing-base:1.5.0
- Added support for AWS SQS
- Improve logging
- Fixed parameter to RabbitMQ
- Added assertions

## [1.5.3] - 2020-02-19

### Changed in 1.5.3

- Improve logging
- Fix parameter to RabbitMQ
- Add assertions

## [1.5.2] - 2020-02-19

### Changed in 1.5.2

- "future-proofed" Governor and InfoFilter
- Refactored: Renamed "with_info" to "withinfo
- Fixed messages

## [1.5.1] - 2020-02-10

### Changed in 1.5.1

- Added "info" and "failure" queues.

## [1.5.0] - 2020-02-09

### Changed in 1.5.0

- Added "info" and "failure" queues.

## [1.4.0] - 2020-01-29

### Changed in 1.4.0

- Update to senzing/senzing-base:1.4.0

## [1.3.3] - 2020-01-28

### Fixed in 1.3.3

- Governor:  Improve filename to avoid import conflict.

## [1.3.2] - 2020-01-27

### Added in 1.3.2

- Support for `SENZING_RABBITMQ_PREFETCH_COUNT`
- Governor:  A "hook" for an external class called before record is consumed

## [1.3.1] - 2020-01-09

### Added in 1.3.1

- Support for `SENZING_ENGINE_CONFIGURATION_JSON`

## [1.3.0] - 2019-11-13

### Added in 1.3.0

- Added MSSQL support
- Update to senzing/senzing-base:1.3.0

## [1.2.1] - 2019-10-10

### Changed in 1.2.1

- Improved license expiry warning
- Remove errant exception catching

## [1.2.0] - 2019-08-19

### Changed in 1.2.0

- RPM based installation

## [1.1.1] - 2019-07-23

### Added in 1.1.1

- Added the ability to delay processing via `SENZING_DELAY_IN_SECONDS`

## [1.1.0] - 2019-07-23

### Added in 1.1.0

- Based on `senzing/senzing-base:1.1.0`, a non-root, immutable container

## [1.0.1] - 2019-07-10

### Changed in 1.0.1

- Migrate to python3

## [1.0.0] - 2019-07-10

### Added in 1.0.0

- Python 2.7 implementation.
- Support Kafka consumer.
- Support RabbitMQ consumer.
