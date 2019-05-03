# stream-loader

## Overview

The [stream-loader.py](stream-loader.py) python script consumes data from various sources (Kafka, URLs) and publishes it to Senzing.
The `senzing/stream-loader` docker image is a wrapper for use in docker formations (e.g. docker-compose, kubernetes).

To see all of the subcommands, run:

```console
$ ./stream-loader.py --help
usage: stream-loader.py [-h] {kafka,sleep,url,version,kafka-test} ...

Load Senzing from a stream. For more information, see
https://github.com/senzing/stream-loader

positional arguments:
  {kafka,sleep,url,version,kafka-test}
                        Subcommands (SENZING_SUBCOMMAND):
    kafka               Read JSON Lines from Apache Kafka topic.
    sleep               Do nothing but sleep. For Docker testing.
    url                 Read JSON Lines from URL-addressable file.
    version             Print version of stream-loader.py.
    kafka-test          Read JSON Lines from Apache Kafka topic. Do not send
                        to Senzing.
    rabbitmq            Read JSON Lines from RabbitMQ queue.
    rabbitmq-test       Read JSON Lines from RabbitMQ. Do not send to Senzing.

optional arguments:
  -h, --help            show this help message and exit
```

To see the options for a subcommand, run commands like:

```console
./stream-loader.py kafka --help
```

### Contents

1. [Expectations](#expectations)
    1. [Space](#space)
    1. [Time](#time)
    1. [Background knowledge](#background-knowledge)
1. [Demonstrate using Command Line](#demonstrate-using-command-line)
    1. [Install](#install)
1. [Demonstrate using Docker](#demonstrate-using-docker)
    1. [Create SENZING_DIR](#create-senzing_dir)
    1. [Configuration](#configuration)
    1. [Run docker container](#run-docker-container)
1. [Develop](#develop)
    1. [Prerequisite software](#prerequisite-software)
    1. [Clone repository](#clone-repository)
    1. [Build docker image for development](#build-docker-image-for-development)
1. [Examples](#examples)
1. [Errors](#errors)

## Expectations

### Space

This repository and demonstration require 6 GB free disk space.

### Time

Budget 40 minutes to get the demonstration up-and-running, depending on CPU and network speeds.

### Background knowledge

This repository assumes a working knowledge of:

1. [Docker](https://github.com/Senzing/knowledge-base/blob/master/WHATIS/docker.md)

## Demonstrate using Command Line

### Install

1. Install prerequisites:
    1. [Debian-based installation](docs/debian-based-installation.md) - For Ubuntu and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based)
    1. [RPM-based installation](docs/rpm-based-installation.md) - For Red Hat, CentOS, openSuse and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based).
1. Install mock-data-generator
    1. See [github.com/Senzing/mock-data-generator](https://github.com/Senzing/mock-data-generator#using-command-line)

## Demonstrate using Docker

### Create SENZING_DIR

1. If `/opt/senzing` directory is not on local system, visit
   [HOWTO - Create SENZING_DIR](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/create-senzing-dir.md).

### Configuration

* **SENZING_DATA_SOURCE** -
  Default "DATA_SOURCE" value for incoming records.
  No default.
* **SENZING_DATABASE_URL** -
  Database URI in the form: `${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}`
  Default:  [internal SQLite database]  
* **SENZING_DEBUG** -
  Enable debug information. Values: 0=no debug; 1=debug.
  Default: 0.
* **SENZING_DIR** -
  Path on the local system where
  [Senzing_API.tgz](https://s3.amazonaws.com/public-read-access/SenzingComDownloads/Senzing_API.tgz)
  has been extracted.
  See [Create SENZING_DIR](#create-senzing_dir).
  No default.
  Usually set to "/opt/senzing".
* **SENZING_ENTITY_TYPE** -
  Default "ENTITY_TYPE" value for incoming records.
  No default.
* **SENZING_ENTRYPOINT_SLEEP** -
  Sleep, in seconds, before executing.
  0 for sleeping infinitely.
  [not-set] if no sleep.
  Useful for debugging docker containers.
  To stop sleeping, run "`unset SENZING_ENTRYPOINT_SLEEP`".
  Default: [not-set].  
* **SENZING_INPUT_URL** -
  URL of source file.
  No default.
* **SENZING_KAFKA_BOOTSTRAP_SERVER** -
  Hostname and port of Kafka server.
  Default: "localhost:9092"
* **SENZING_KAFKA_GROUP** -
  Kafka group.
  Default: "senzing-kafka-group"
* **SENZING_KAFKA_TOPIC** -
  Kafka topic.
  Default: "senzing-kafka-topic"
* **SENZING_LOG_LEVEL** -
  Level of logging. {notset, debug, info, warning, error, critical}.
  Default: info
* **SENZING_MONITORING_PERIOD** -
  Time, in seconds, between monitoring log records.
  Default: 300
* **SENZING_PROCESSES** -
  Number of processes to allocated for processing.
  Default: 1
* **SENZING_QUEUE_MAX** -
  Maximum items for internal queue.
  Default: 10
* **SENZING_RABBITMQ_HOST** -
  Host name of the RabbitMQ exchange.
  Default: "localhost:5672"
* **SENZING_RABBITMQ_PASSWORD** -
  The password for the RabbitMQ queue.
  Default: "bitnami"
* **SENZING_RABBITMQ_QUEUE** -
  Name of the RabbitMQ queue used for communication.
  Default: "senzing-rabbitmq-queue"
* **SENZING_RABBITMQ_USERNAME** -
  The username for the RabbitMQ queue.
  Default: "user"
* **SENZING_SLEEP_TIME** -
  Amount of time to sleep, in seconds for `stream-loader.py sleep` subcommand.
  Default: 600.
* **SENZING_SUBCOMMAND** -
  Identify the subcommand to be run. See `stream-loader.py --help` for complete list.  
* **SENZING_THREADS_PER_PROCESS** -
  Number of threads per process to allocate for processing.
  Default: 4
  
1. To determine which configuration parameters are use for each `<subcommand>`, run:

    ```console
    ./stream-loader.py <subcommand> --help
    ```

### Run docker container

#### Demonstrate Kafka to Senzing

1. :pencil2: Determine docker network.  Example:

    ```console
    sudo docker network ls

    # Choose value from NAME column of docker network ls
    export SENZING_NETWORK=nameofthe_network
    ```

1. :pencil2: Set environment variables.  Example:

    ```console
    export DATABASE_PROTOCOL=mysql
    export DATABASE_USERNAME=g2
    export DATABASE_PASSWORD=g2
    export DATABASE_HOST=senzing-mysql
    export DATABASE_PORT=3306
    export DATABASE_DATABASE=G2

    export SENZING_SUBCOMMAND=kafka
    export SENZING_DATA_SOURCE=PEOPLE
    export SENZING_DIR=/opt/senzing
    export SENZING_KAFKA_BOOTSTRAP_SERVER=senzing-kafka:9092
    export SENZING_KAFKA_TOPIC=senzing-kafka-topic
    export SENZING_MONITORING_PERIOD=60
    ```

1. Run the docker container.  Example:

    ```console
    export SENZING_DATABASE_URL="${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}"

    sudo docker run \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_DATABASE_URL="${SENZING_DATABASE_URL}" \
      --env SENZING_DATA_SOURCE="${SENZING_DATA_SOURCE}" \
      --env SENZING_KAFKA_BOOTSTRAP_SERVER="${SENZING_KAFKA_BOOTSTRAP_SERVER}" \
      --env SENZING_KAFKA_TOPIC="${SENZING_KAFKA_TOPIC}" \
      --env SENZING_MONITORING_PERIOD="${SENZING_MONITORING_PERIOD}" \
      --interactive \
      --net ${SENZING_NETWORK} \
      --rm \
      --tty \
      --volume ${SENZING_DIR}:/opt/senzing \
      senzing/stream-loader
    ```

## Develop

### Prerequisite software

The following software programs need to be installed:

1. [git](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-git.md)
1. [make](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-make.md)
1. [docker](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-docker.md)

### Clone repository

1. Set these environment variable values:

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=stream-loader
    ```

1. Follow steps in [clone-repository](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/clone-repository.md) to install the Git repository.

1. After the repository has been cloned, be sure the following are set:

    ```console
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    ```

### Build docker image for development

1. Option #1 - Using docker command and GitHub.

    ```console
    sudo docker build --tag senzing/stream-loader https://github.com/senzing/docker-template.git
    ```

1. Option #2 - Using docker command and local repository.

    ```console
    cd ${GIT_REPOSITORY_DIR}
    sudo docker build --tag senzing/stream-loader .
    ```

1. Option #3 - Using make command.

    ```console
    cd ${GIT_REPOSITORY_DIR}
    sudo make docker-build
    ```

## Examples

1. Examples of use:
    1. [docker-compose-demo](https://github.com/Senzing/docker-compose-demo)
    1. [kubernetes-demo](https://github.com/Senzing/kubernetes-demo)
    1. [rancher-demo](https://github.com/Senzing/rancher-demo/tree/master/docs/db2-cluster-demo.md)

## Errors

1. See [docs/errors.md](docs/errors.md).
