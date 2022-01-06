ARG BASE_IMAGE=debian:10.11@sha256:94ccfd1c5115a6903cbb415f043a0b04e307be3f37b768cf6d6d3edff0021da3
FROM ${BASE_IMAGE} AS builder

ENV REFRESHED_AT=2022-01-06

LABEL Name="senzing/stream-loader" \
      Maintainer="support@senzing.com" \
      Version="1.9.2"

# Run as "root" for system installation.
USER root

RUN apt-get update \
 && apt-get -y install \
    python3 \
    python3-dev \
    python3-venv \
    python3-pip \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# create and activate virtual environment
RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Install packages via PIP.
COPY requirements.txt .
RUN pip3 install --upgrade pip \
 && pip3 install -r requirements.txt \
 && rm /requirements.txt

##
# create the runtime image
FROM ${BASE_IMAGE} AS runner

# Define health check

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
      librdkafka-dev \
      libxml2 \
      python3 \
      python3-venv \
      postgresql-client \
      unixodbc \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Copy files from repository.
COPY ./rootfs /
COPY ./stream-loader.py /app/

# Copy python virtual environment from the builder image
COPY --from=builder /app/venv /app/venv

# Make non-root container.
USER 1001

# make sure all messages always reach console
ENV PYTHONUNBUFFERED=1

# activate virtual environment
ENV VIRTUAL_ENV=/app/venv
ENV PATH="/app/venv/bin:$PATH"

# Runtime execution.
ENV SENZING_DOCKER_LAUNCHED=true
ENV LD_LIBRARY_PATH=/opt/senzing/g2/lib:/opt/senzing/g2/lib/debian:/opt/IBM/db2/clidriver/lib
ENV PATH=${PATH}:/opt/senzing/g2/python:/opt/IBM/db2/clidriver/adm:/opt/IBM/db2/clidriver/bin
ENV PYTHONPATH=/opt/senzing/g2/python
ENV SENZING_ETC_PATH=/etc/opt/senzing

WORKDIR /app
ENTRYPOINT ["/app/stream-loader.py"]
