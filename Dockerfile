ARG BASE_IMAGE=senzing/python-base
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2019-03-14

LABEL Name="senzing/stream-loader" \
      Version="1.0.0"

RUN apt-get update \
 && apt-get -y install \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# Perform PIP installs.

RUN pip install \
    configparser \
    confluent-kafka \
    psutil

# Copy into the app directory.

COPY ./stream-loader.py /app/

# Override parent docker image.

WORKDIR /app
ENTRYPOINT ["/app/docker-entrypoint.sh", "/app/stream-loader.py" ]
CMD [""]
