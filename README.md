# stream-loader

## Overview

The [stream-loader.py](stream-loader.py) python script consumes data from various sources (Kafka, STDIN) and publishes it to Senzing.
The `senzing/stream-loader` docker images is a wrapper for use in docker formations (e.g. docker-compose, kubernetes).

To see all of the subcommands, run:

```console
$ ./stream-loader.py --help
usage: stream-loader.py [-h] {kafka,sleep,stdin,test,url,version} ...

Load Senzing from a stream. For more information, see
https://github.com/senzing/stream-loader

positional arguments:
  {kafka,sleep,stdin,test,url,version}
                        Subcommands (SENZING_SUBCOMMAND):
    kafka               Read JSON Lines from Apache Kafka topic.
    sleep               Do nothing but sleep. For Docker testing.
    stdin               Read JSON Lines from STDIN.
    test                Read JSON Lines from STDIN. No changes to Senzing.
    url                 Read JSON Lines from URL-addressable file.
    version             Print version of stream-loader.py.

optional arguments:
  -h, --help            show this help message and exit
```

To see the options for a subcommand, run commands like:

```console
./stream-loader.py test --help
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
    1. [Debian-based installation](doc/debian-based-installation.md) - For Ubuntu and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based)
    1. [RPM-based installation](doc/rpm-based-installation.md) - For Red Hat, CentOS, openSuse and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based).
1. Install mock-data-generator
    1. See [github.com/Senzing/mock-data-generator](https://github.com/Senzing/mock-data-generator#using-command-line)

### Demonstrate

1. See [installation instructions](#install) for setting `GIT_REPOSITORY_DIR`.

1. Show help.  Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./stream-loader.py --help
    ./stream-loader.py kafka --help
    ```

1. Show file sent to `stream-loader.py`'s test via URL. No changes are made to Senzing. Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./stream-loader.py test \
      --input-url "https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json"
    ```

1. Show random mock data sent to `stream-loader.py`'s test. No changes are made to Senzing. Example:

    ```console
    export MOCK_DATA_GENERATOR_PATH=~/senzing.git/mock-data-generator

    cd ${GIT_REPOSITORY_DIR}
    ${MOCK_DATA_GENERATOR_PATH}/mock-data-generator.py random-to-stdout \
      --random-seed 22 \
      --record-min 1 \
      --record-max 10 \
      --records-per-second 2 \
    | \
    ./stream-loader.py test
    ```

1. Show file-based mock data sent to `stream-loader.py`'s test via STDIN/STDOUT. No changes are made to Senzing. Example:

    ```console
    export MOCK_DATA_GENERATOR_PATH=~/senzing.git/mock-data-generator

    cd ${GIT_REPOSITORY_DIR}
    ${MOCK_DATA_GENERATOR_PATH}/mock-data-generator.py url-to-stdout \
      --input-url https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json \
      --record-min 1 \
      --record-max 10 \
      --records-per-second 2 \
    | \
    ./stream-loader.py test
    ```

## Using Docker

### Build docker image

1. Build docker image.

    ```console
    sudo docker build --tag senzing/stream-loader https://github.com/senzing/stream-loader.git
    ```

### Configuration

- `SENZING_SUBCOMMAND` - Identify the subcommand to be run. See `stream-loader.py --help` for complete list.

1. To determine which configuration parameters are use for each `<subcommand>`, run:

    ```console
    ./stream-loader.py <subcommand> --help
    ```

    - `DATABASE_USERNAME` - Database username.
    - `DATABASE_PASSWORD` - Database password.
    - `DATABASE_PROTOCOL` - Database protocol.  (e.g. "mysql").
    - `DATABASE_HOST` - Hostname of database service.
    - `DATABASE_PORT` - Port of database service.
    - `DATABASE_DATABASE` - Database name.
    - `SENZING_DEBUG` - Print debug statements to log.  Default: False
    - `SENZING_DATA_SOURCE` - Default "DATA_SOURCE" value for incoming records.
    - `SENZING_DIR` - Location of Senzing libraries. Default: "/opt/senzing".
    - `SENZING_ENTITY_TYPE` - Default "ENTITY_TYPE" value for incoming records.
    - `SENZING_INPUT_URL` - URL of source file. Default: [https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json](https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json)
    - `SENZING_KAFKA_BOOTSTRAP_SERVER` - Hostname and port of Kafka server.  Default: "localhost")
    - `SENZING_KAFKA_GROUP` - Kafka group. Default: "senzing-kafka-group"
    - `SENZING_KAFKA_TOPIC` - Kafka topic. Default: "senzing-kafka-topic"  
    - `SENZING_MONITORING_PERIOD` - Time, in seconds, between monitoring log records. Default: 300

### Run docker image

#### Demonstrate URL test

1. Run the docker container. No changes are made to Senzing. Example:

    ```console
    export SENZING_SUBCOMMAND=test

    export SENZING_DIR=/opt/senzing
    export SENZING_INPUT_URL=https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json

    sudo docker run -it  \
      --volume ${SENZING_DIR}:/opt/senzing \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_INPUT_URL="${SENZING_INPUT_URL}" \
      senzing/stream-loader
    ```

#### Demonstrate URL to Senzing

1. Determine docker network:

    ```console
    docker network ls

    # Choose value from NAME column of docker network ls
    export SENZING_NETWORK=nameofthe_network
    ```

1. Run the docker container. Example:

    ```console
    export SENZING_SUBCOMMAND=url

    export DATABASE_PROTOCOL=mysql
    export DATABASE_USERNAME=g2
    export DATABASE_PASSWORD=g2
    export DATABASE_HOST=senzing-mysql
    export DATABASE_PORT=3306
    export DATABASE_DATABASE=G2
    export SENZING_DATA_SOURCE=PEOPLE
    export SENZING_DIR=/opt/senzing
    export SENZING_INPUT_URL=https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json
    export SENZING_MONITORING_PERIOD=5

    sudo docker run -it \
      --volume ${SENZING_DIR}:/opt/senzing \
      --net ${SENZING_NETWORK} \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_DATABASE_URL="${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}" \
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
    export SENZING_SUBCOMMAND=kafka

    export DATABASE_PROTOCOL=mysql
    export DATABASE_USERNAME=g2
    export DATABASE_PASSWORD=g2
    export DATABASE_HOST=senzing-mysql
    export DATABASE_PORT=3306
    export DATABASE_DATABASE=G2
    export SENZING_DATA_SOURCE=PEOPLE
    export SENZING_DIR=/opt/senzing
    export SENZING_KAFKA_BOOTSTRAP_SERVER=senzing-kafka:9092
    export SENZING_KAFKA_TOPIC=senzing-kafka-topic
    export SENZING_MONITORING_PERIOD=5

    sudo docker run -it \
      --volume ${SENZING_DIR}:/opt/senzing \
      --net ${SENZING_NETWORK} \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_DATABASE_URL="${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}" \
      --env SENZING_DATA_SOURCE="${SENZING_DATA_SOURCE}" \
      --env SENZING_KAFKA_BOOTSTRAP_SERVER="${SENZING_KAFKA_BOOTSTRAP_SERVER}" \
      --env SENZING_KAFKA_TOPIC="${SENZING_KAFKA_TOPIC}" \
      --env SENZING_MONITORING_PERIOD="${SENZING_MONITORING_PERIOD}" \
      senzing/stream-loader
    ```
  
## Errors

1. See [doc/errors.md](doc/errors.md).