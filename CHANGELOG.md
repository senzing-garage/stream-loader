# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
