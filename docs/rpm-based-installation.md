# RPM-based installation

The following instructions are meant to be "copy-and-paste" to install and demonstrate.
If a step requires you to think and make a decision, it will be prefaced with :warning:.

The instructions have been tested against a bare
[CentOS-7-x86_64-Minimal-1511.iso](http://archive.kernel.org/centos-vault/7.2.1511/isos/x86_64/CentOS-7-x86_64-Minimal-1511.iso)
image.

## Overview

1. [Install prerequisites](#prerequisites)
1. [Set environment variables](#set-environment-variables)
1. [Clone repository](#clone-repository)
1. [Install](#install)

## Prerequisites

1. YUM installs

    ```console
    sudo yum -y install epel-release
    sudo yum -y install git
    ```

## Set Environment variables

1. :warning: Set environment variables.
   These variables may be modified, but do not need to be modified.
   The variables are used throughout the installation procedure.

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=stream-loader
    export SENZING_DIR=/opt/senzing
    ```

1. Synthesize environment variables.

    ```console
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    export GIT_REPOSITORY_URL="https://github.com/${GIT_ACCOUNT}/${GIT_REPOSITORY}.git"
    export LD_LIBRARY_PATH=${SENZING_DIR}/g2/lib:$LD_LIBRARY_PATH
    export PYTHONPATH=${SENZING_DIR}/g2/python
    ```

## Clone repository

1. Get repository.

    ```console
    mkdir --parents ${GIT_ACCOUNT_DIR}
    cd  ${GIT_ACCOUNT_DIR}
    git clone ${GIT_REPOSITORY_URL}
    ```

## Install

### APT installs

1. Run:

    ```console
    sudo xargs yum -y install < ${GIT_REPOSITORY_DIR}/src/yum-packages.txt
    ```

### PIP installs

1. Run:

    ```console
    sudo pip install -r ${GIT_REPOSITORY_DIR}/requirements.txt
    ```

### Create SENZING_DIR

If you do not already have an `/opt/senzing` directory on your local system, visit
[HOWTO - Create SENZING_DIR](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/create-senzing-dir.md).
