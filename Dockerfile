ARG BASE_IMAGE=senzing/senzing-base
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2019-07-10

LABEL Name="senzing/stream-loader" \
      Maintainer="support@senzing.com" \
      Version="1.0.1"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# Install packages via PIP.

RUN pip3 install \
    configparser \
    confluent-kafka \
    psutil \
    pika

# Copy files from repository.

COPY ./rootfs /
COPY ./stream-loader.py /app/

# Runtime execution.

ENV SENZING_DOCKER_LAUNCHED=true

WORKDIR /app
ENTRYPOINT ["/app/docker-entrypoint.sh", "/app/stream-loader.py" ]
CMD [""]
