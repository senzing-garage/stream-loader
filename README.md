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

optional arguments:
  -h, --help            show this help message and exit
```

To see the options for a subcommand, run commands like:

```console
./stream-loader.py kafka --help
```

### Contents

1. [Using Command Line](#using-command-line)
    1. [Install](#install)
    1. [Demonstrate](#demonstrate)
1. [Using Docker](#using-docker)
    1. [Build docker image](#build-docker-image)
    1. [Configuration](#configuration)
    1. [Run docker image](#run-docker-image)
1. [Errors](errors)

## Using Command Line

### Install

1. Install prerequisites:
    1. [Debian-based installation](docs/debian-based-installation.md) - For Ubuntu and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based)
    1. [RPM-based installation](docs/rpm-based-installation.md) - For Red Hat, CentOS, openSuse and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based).
1. Install mock-data-generator
    1. See [github.com/Senzing/mock-data-generator](https://github.com/Senzing/mock-data-generator#using-command-line)

### Demonstrate

## Using Docker

### Build docker image

1. Build a [senzing/python-base](https://github.com/Senzing/docker-python-base) docker image.  Example:

    ```console
    export BASE_IMAGE=senzing/python-mysql-base

    sudo docker build \
      --tag ${BASE_IMAGE} \
      https://github.com/senzing/docker-python-mysql-base.git
    ```

1. Build docker image. Example:

    ```console
    export BASE_IMAGE=senzing/python-mysql-base

    sudo docker build \
      --tag senzing/stream-loader \
      --build-arg BASE_IMAGE=${BASE_IMAGE} \
      https://github.com/senzing/stream-loader.git
    ```

### Configuration

- **SENZING_SUBCOMMAND** -
  Identify the subcommand to be run. See `stream-loader.py --help` for complete list.
- **SENZING_DATA_SOURCE** -
  Default "DATA_SOURCE" value for incoming records.
- **SENZING_DIR** -
  Location of Senzing libraries. Default: "/opt/senzing".
- **SENZING_ENTITY_TYPE** -
  Default "ENTITY_TYPE" value for incoming records.
- **SENZING_INPUT_URL** -
  URL of source file.
- **SENZING_KAFKA_BOOTSTRAP_SERVER** -
  Hostname and port of Kafka server.  Default: "localhost:9092"
- **SENZING_KAFKA_GROUP** -
  Kafka group. Default: "senzing-kafka-group"
- **SENZING_KAFKA_TOPIC** -
  Kafka topic. Default: "senzing-kafka-topic"
- **SENZING_LOG_LEVEL** -
  Level of logging. {notset, debug, info, warning, error, critical}. Default: info
- **SENZING_MONITORING_PERIOD** -
  Time, in seconds, between monitoring log records. Default: 300
- **SENZING_PROCESSES** -
  Number of processes to allocated for processing. Default: 1
- **SENZING_QUEUE_MAX** -
  Maximum items for internal queue. Default: 10
- **SENZING_SUBCOMMAND** -
  Amount of time to sleep, in seconds for `stream-loader.py sleep` subcommand. Default: 600.
- **SENZING_THREADS_PER_PROCESS** -
  Number of threads per process to allocate for processing. Default: 4
  
1. To determine which configuration parameters are use for each `<subcommand>`, run:

    ```console
    ./stream-loader.py <subcommand> --help
    ```

### Run docker image

#### Demonstrate URL to Senzing

1. Determine docker network:

    ```console
    docker network ls

    # Choose value from NAME column of docker network ls
    export SENZING_NETWORK=nameofthe_network
    ```

1. Run the docker container. Example:

    ```console
    export DATABASE_PROTOCOL=mysql
    export DATABASE_USERNAME=g2
    export DATABASE_PASSWORD=g2
    export DATABASE_HOST=senzing-mysql
    export DATABASE_PORT=3306
    export DATABASE_DATABASE=G2

    export SENZING_SUBCOMMAND=url
    export SENZING_DATABASE_URL="${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}"
    export SENZING_DATA_SOURCE=PEOPLE
    export SENZING_DIR=/opt/senzing
    export SENZING_INPUT_URL=https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json
    export SENZING_MONITORING_PERIOD=60
    
    sudo docker run -it \
      --volume ${SENZING_DIR}:/opt/senzing \
      --net ${SENZING_NETWORK} \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_DATABASE_URL="${SENZING_DATABASE_URL}" \
      --env SENZING_DATA_SOURCE="${SENZING_DATA_SOURCE}" \
      --env SENZING_INPUT_URL="${SENZING_INPUT_URL}" \
      --env SENZING_MONITORING_PERIOD="${SENZING_MONITORING_PERIOD}" \
      senzing/stream-loader
    ```

#### Demonstrate Kafka to Senzing

1. Determine docker network:

    ```console
    docker network ls

    # Choose value from NAME column of docker network ls
    export SENZING_NETWORK=nameofthe_network
    ```

1. Run the docker container. Example:

    ```console
    export DATABASE_PROTOCOL=mysql
    export DATABASE_USERNAME=g2
    export DATABASE_PASSWORD=g2
    export DATABASE_HOST=senzing-mysql
    export DATABASE_PORT=3306
    export DATABASE_DATABASE=G2

    export SENZING_SUBCOMMAND=kafka
    export SENZING_DATABASE_URL="${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}"
    export SENZING_DATA_SOURCE=PEOPLE
    export SENZING_DIR=/opt/senzing
    export SENZING_KAFKA_BOOTSTRAP_SERVER=senzing-kafka:9092
    export SENZING_KAFKA_TOPIC=senzing-kafka-topic
    export SENZING_MONITORING_PERIOD=60

    sudo docker run -it \
      --volume ${SENZING_DIR}:/opt/senzing \
      --net ${SENZING_NETWORK} \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_DATABASE_URL="${SENZING_DATABASE_URL}" \
      --env SENZING_DATA_SOURCE="${SENZING_DATA_SOURCE}" \
      --env SENZING_KAFKA_BOOTSTRAP_SERVER="${SENZING_KAFKA_BOOTSTRAP_SERVER}" \
      --env SENZING_KAFKA_TOPIC="${SENZING_KAFKA_TOPIC}" \
      --env SENZING_MONITORING_PERIOD="${SENZING_MONITORING_PERIOD}" \
      senzing/stream-loader
    ```

## Alternative Docker builds

### stream-loader-db2-cluster

This build is for use with a DB2 Cluster.

Build steps:

1. Build [senzing/python-db2-cluster-base](https://github.com/Senzing/docker-python-db2-cluster-base).
1. Build `senzing/stream-loader-db2-cluster`.

    ```console
    sudo docker build \
      --build-arg BASE_IMAGE=senzing/python-db2-cluster-base \
      --tag senzing/stream-loader-db2-cluster \
      https://github.com/senzing/stream-loader.git
    ```

Examples of use:

1. [docker-compose-db2-cluster-demo](https://github.com/Senzing/docker-compose-db2-cluster-demo)
1. [rancher-demo](https://github.com/Senzing/rancher-demo/tree/master/docs/db2-cluster-demo.md)

## Errors

1. See [docs/errors.md](docs/errors.md).