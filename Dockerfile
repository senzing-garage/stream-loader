ARG BASE_IMAGE=senzing/senzing-base:1.5.4
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2020-09-24

LABEL Name="senzing/stream-loader" \
      Maintainer="support@senzing.com" \
      Version="1.7.1"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# Install packages via PIP.

RUN pip3 install \
      boto3 \
      configparser \
      confluent-kafka \
      pika \
      psutil \
      psycopg2-binary

# Copy files from repository.

COPY ./rootfs /
COPY ./stream-loader.py /app/

# Make non-root container.

USER 1001

# Runtime execution.

ENV SENZING_DOCKER_LAUNCHED=true

WORKDIR /app
ENTRYPOINT ["/app/stream-loader.py"]
