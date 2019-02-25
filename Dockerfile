FROM senzing/python-base

ENV REFRESHED_AT=2018-11-08

RUN yum -y update; yum clean all
RUN yum -y install epel-release; yum clean all
RUN yum -y install \
    curl \
    librdkafka-devel \
    python-devel \
    python-pip; \
    yum clean all

# Perform PIP installs

RUN pip install \
    configparser \
    confluent-kafka \
    psutil

# Copy into the app directory.

COPY ./stream-loader.py /app/

# Override parent docker image.

WORKDIR /app
ENTRYPOINT ["/app/stream-loader.py"]
