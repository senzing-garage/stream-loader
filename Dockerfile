ARG BASE_IMAGE=senzing/senzingapi-runtime:3.3.2

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM ${BASE_IMAGE} AS builder

ENV REFRESHED_AT=2022-10-27

LABEL Name="senzing/stream-loader" \
      Maintainer="support@senzing.com" \
      Version="2.2.0"

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt update \
 && apt -y install \
      curl \
      libaio1 \
      python3 \
      python3-dev \
      python3-pip \
      python3-venv \
 && apt clean \
 && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment.

RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Install packages via PIP.

COPY requirements.txt .
RUN pip3 install --upgrade pip \
 && pip3 install -r requirements.txt \
 && rm requirements.txt

# Install senzing_governor.py.

RUN curl -X GET \
      --output /opt/senzing/g2/sdk/python/senzing_governor.py \
      https://raw.githubusercontent.com/Senzing/governor-postgresql-transaction-id/main/senzing_governor.py

# -----------------------------------------------------------------------------
# Stage: Final
# -----------------------------------------------------------------------------

# Create the runtime image.

FROM ${BASE_IMAGE} AS runner

ENV REFRESHED_AT=2022-10-27

LABEL Name="senzing/stream-loader" \
      Maintainer="support@senzing.com" \
      Version="2.2.0"

# Define health check.

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt update \
 && apt -y install \
      libaio1 \
      libodbc1 \
      librdkafka-dev \
      libxml2 \
      postgresql-client \
      python3 \
      python3-venv \
      unixodbc \
 && apt clean \
 && rm -rf /var/lib/apt/lists/*

# Copy files from repository.

COPY ./rootfs /
COPY ./stream-loader.py /app/

# Copy python virtual environment from the builder image.

COPY --from=builder /app/venv /app/venv
COPY --from=builder /opt/senzing/g2/sdk/python/senzing_governor.py /opt/senzing/g2/sdk/python/senzing_governor.py

# Make non-root container.

USER 1001

# Activate virtual environment.

ENV VIRTUAL_ENV=/app/venv
ENV PATH="/app/venv/bin:${PATH}"

# Runtime environment variables.

ENV LD_LIBRARY_PATH=/opt/senzing/g2/lib:/opt/senzing/g2/lib/debian:/opt/IBM/db2/clidriver/lib
ENV PATH=${PATH}:/opt/senzing/g2/python:/opt/IBM/db2/clidriver/adm:/opt/IBM/db2/clidriver/bin
ENV PYTHONPATH=/opt/senzing/g2/sdk/python
ENV PYTHONUNBUFFERED=1
ENV SENZING_DOCKER_LAUNCHED=true

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["/app/stream-loader.py"]
