# Debian-based installation

The following instructions are meant to be "copy-and-paste" to install and demonstrate.
If a step requires you to think and make a decision, it will be prefaced with :warning:.

The instructions have been tested against a bare
[ubuntu-18.04.1-server-amd64.iso](http://cdimage.ubuntu.com/ubuntu/releases/bionic/release/ubuntu-18.04.1-server-amd64.iso)
image.

## Overview

1. [Prerequisites](#prerequisites)
1. [Clone repository](#clone-repository)
1. [Install](#install)
1. [Set environment variables](#set-environment-variables)

## Prerequisites

1. APT installs

    ```console
    sudo apt update
    sudo apt -y install git
    ```

## Clone repository

For more information on environment variables,
see [Environment Variables](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md).

1. Set these environment variable values:

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=stream-loader
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    ```

1. Follow steps in [clone-repository](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/clone-repository.md) to install the Git repository.

## Install

### APT installs

1. Run:

    ```console
    sudo xargs apt -y install < ${GIT_REPOSITORY_DIR}/src/apt-packages.txt
    ```

### PIP installs

1. Run:

    ```console
    sudo pip install -r ${GIT_REPOSITORY_DIR}/requirements.txt
    ```

### Senzing install

1. If Senzing has not been installed, visit
   [HOWTO - Initialize Senzing](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/initialize-senzing.md).

## Set Environment variables

1. :pencil2: Location of Senzing G2 directory.
   Example:

    ```console
    export SENZING_G2_DIR=/opt/senzing/g2
    ```

1. Synthesize environment variables.

    ```console
    export LD_LIBRARY_PATH=${SENZING_G2_DIR}/lib:${SENZING_G2_DIR}/lib/debian:$LD_LIBRARY_PATH
    export PYTHONPATH=${SENZING_G2_DIR}/python
    ```
