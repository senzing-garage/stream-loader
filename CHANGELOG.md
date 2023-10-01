# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

-

## [2.2.9] - 2023-09-30

### Changed in 2.2.9

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.7.1`
- In `requirements.txt`, updated:
  - azure-servicebus==7.11.2
  - boto3==1.28.57
  - confluent-kafka==2.2.0
  - orjson==3.9.7
  - psycopg2-binary==2.9.8

## [2.2.8] - 2023-06-29

### Changed in 2.2.8

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.6.0`
- In `requirements.txt`, updated:
  - boto3==1.26.163

## [2.2.7] - 2023-06-15

### Changed in 2.2.7

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.5.3`
- In `requirements.txt`, updated:
  - azure-servicebus==7.11.0
  - boto3==1.26.153
  - orjson==3.9.1

## [2.2.6] - 2023-05-09

### Changed in 2.2.6

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.5.2`
- In `requirements.txt`, updated:
  - azure-servicebus==7.9.0
  - boto3==1.26.130
  - confluent-kafka==2.1.1
  - orjson==3.8.12
  - pika==1.3.2
  - psutil==5.9.5
  - psycopg2-binary==2.9.6

## [2.2.5] - 2023-04-03

### Changed in 2.2.5

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.5.0`
- In `requirements.txt`, updated:
  - azure-servicebus==7.8.3
  - boto3==1.26.104
  - confluent-kafka==2.0.2
  - orjson==3.8.9

## [2.2.4] - 2023-01-12

### Changed in 2.2.4

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.4.0`
- In `requirements.txt`, updated:
  - boto3==1.26.48
  - orjson==3.8.5

## [2.2.3] - 2022-12-14

### Changed in 2.2.3

- Updated SQS visibility timeout from 30s to 30m

## [2.2.2] - 2022-11-01

### Changed in 2.2.2

- Fix for `rabbitmq-custom` logging.

## [2.2.1] - 2022-10-27

### Changed in 2.2.1

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.3.2`
- In `requirements.txt`, updated:
  - boto3==1.25.4
  - orjson==3.8.1
  - pika==1.3.1
  - psutil==5.9.3
  - psycopg2-binary==2.9.5

## [2.2.0] - 2022-10-18

### Added in 2.2.0

- Added `rabbitmq-custom` subcommand

## [2.1.2] - 2022-10-11

### Changed in 2.1.2

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.3.1`
- In `requirements.txt`, updated:
  - boto3==1.24.89
  - psycopg2-binary==2.9.4

## [2.1.1] - 2022-09-28

### Changed in 2.1.1

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-tools:3.3.0`
- In `requirements.txt`, updated:
  - boto3==1.24.82
  - psutil==5.9.2

## [2.1.0] - 2022-08-26

### Changed in 2.1.0

- In `Dockerfile`, bump from `senzing/senzingapi-runtime:3.1.1` to `senzing/senzingapi-runtime:3.2.0`
- Updated python dependencies

### Deleted in 2.1.0

- Removed support for `SENZING_DEFAULT_ENTITY_TYPE`
- Deleted `Dockerfile-with-data`

## [2.0.2] - 2022-07-29

### Changed in 2.0.2

- Changed from `SENZING_AZURE_CONNECTION_STRING` to `SENZING_AZURE_QUEUE_CONNECTION_STRING` for clarity

## [2.0.1] - 2022-07-20

### Changed in 2.0.1

- In `Dockerfile`, bump from `senzing/senzingapi-runtime:3.1.0` to `senzing/senzingapi-runtime:3.1.1`

## [2.0.0] - 2022-07-14

### Changed in 2.0.0

- Migrated to `senzing/senzingapi-runtime` as Docker base image

## [1.10.7] - 2022-06-30

### Changed in 1.10.7

- Add Support for `SENZING_LICENSE_BASE64_ENCODED`
- Remove support for `SENZING_ENTITY_TYPE`

## [1.10.6] - 2022-06-28

### Changed in 1.10.6

- Remove support for `SENZING_DATA_SOURCE` parameter
- Upgrade `Dockerfile` to `debian:11.3-slim@sha256:f6957458017ec31c4e325a76f39d6323c4c21b0e31572efa006baa927a160891`
- Increase monitoring check frequency

## [1.10.5] - 2022-06-15

### Changed in 1.10.5

- Fixed issue with records not having `RECORD_ID`

## [1.10.4] - 2022-06-08

### Changed in 1.10.4

- Upgrade `Dockerfile` to `FROM debian:11.3-slim@sha256:06a93cbdd49a265795ef7b24fe374fee670148a7973190fb798e43b3cf7c5d0f`

## [1.10.3] - 2022-05-06

### Changed in 1.10.3

- Added `libodbc1` to Dockerfile

## [1.10.2] - 2022-05-05

### Changed in 1.10.2

- Added `poll()` calls for Kafka's output and failure queues
- Terminate on failure to deliver to info or failure queues.

## [1.10.1] - 2022-04-19

### Changed in 1.10.1

- Added additional logging around calls to Senzing

## [1.10.0] - 2022-04-08

### Changed in 1.10.0

- Migrate from `senzingdata-v2` to `senzingdata-v3`
- In `Dockerfile-with-data`, added docker build args to Dockerfile for more flexibility.
  - SENZING_APT_REPOSITORY_URL
  - SENZING_DATA_PACKAGE_NAME
  - SENZING_DATA_SUBDIRECTORY

## [1.9.10] - 2022-03-24

### Changed in 1.9.10

- Remove entire messages from log as they may contain Personally Identifiable Information (PII)

## [1.9.9] - 2022-03-18

### Changed in 1.9.9

- Support for `libcrypto` and `libssl`

## [1.9.8] - 2022-02-25

### Changed in 1.9.8

- Support for enhanced v3 python package styles

## [1.9.7] - 2022-02-11

### Changed in 1.9.7

- Improved support for Senzing v2 and v3 python package styles

### Added in 1.9.7

- Added label to runner image
- updated documentation

## [1.9.6] - 2022-02-09

### Changed in 1.9.6

- Added label to runner image
- updated documentation

## [1.9.5] - 2022-02-04

### Changed in 1.9.5

- Support for Senzing v2 and v3 python package styles

## [1.9.4] - 2022-01-26

### Added in 1.9.4

- Updated base image to address vulnerabilities
- Added support for Kafka configuration

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
