# Git variables

GIT_REPOSITORY_NAME := $(shell basename `git rev-parse --show-toplevel`)
GIT_VERSION := $(shell git describe --always --tags --long --dirty | sed -e 's/\-0//' -e 's/\-g.......//')

# Docker variables

DOCKER_IMAGE_TAG ?= $(GIT_REPOSITORY_NAME):$(GIT_VERSION)
DOCKER_IMAGE_NAME := senzing/stream-loader
SENZING_ACCEPT_EULA ?= no
SENZING_APT_STAGING_REPOSITORY_URL=https://senzing-staging-apt.s3.amazonaws.com/senzingstagingrepo_1.0.0-1_amd64.deb

# -----------------------------------------------------------------------------
# The first "make" target runs as default.
# -----------------------------------------------------------------------------

.PHONY: default
default: help

# -----------------------------------------------------------------------------
# Docker-based builds
# -----------------------------------------------------------------------------

.PHONY: docker-build
docker-build:
	docker build \
		--tag $(DOCKER_IMAGE_NAME) \
		--tag $(DOCKER_IMAGE_NAME):$(GIT_VERSION) \
		.

.PHONY: docker-build-with-data
docker-build-with-data:
	docker build \
		--build-arg SENZING_ACCEPT_EULA=$(SENZING_ACCEPT_EULA) \
		--file Dockerfile-with-data \
		--tag $(DOCKER_IMAGE_NAME)-with-data \
		--tag $(DOCKER_IMAGE_NAME)-with-data:$(GIT_VERSION) \
		.

.PHONY: docker-build-with-data-staging
docker-build-with-data-staging:
	docker build \
		--build-arg SENZING_ACCEPT_EULA=$(SENZING_ACCEPT_EULA) \
		--build-arg SENZING_APT_REPOSITORY_URL=$(SENZING_APT_STAGING_REPOSITORY_URL) \
		--file Dockerfile-with-data \
		--tag $(DOCKER_IMAGE_NAME)-with-data-staging \
		--tag $(DOCKER_IMAGE_NAME)-with-data-staging:$(GIT_VERSION) \
		.

# -----------------------------------------------------------------------------
# Clean up targets
# -----------------------------------------------------------------------------

.PHONY: docker-rmi-for-build
docker-rmi-for-build:
	-docker rmi --force \
		$(DOCKER_IMAGE_NAME):$(GIT_VERSION) \
		$(DOCKER_IMAGE_NAME)

.PHONY: docker-rmi-for-build-with-data
docker-rmi-for-build-with-data:
	-docker rmi --force \
		$(DOCKER_IMAGE_NAME)-with-data:$(GIT_VERSION) \
		$(DOCKER_IMAGE_NAME)-with-data

.PHONY: docker-rmi-for-build-with-data-staging
docker-rmi-for-build-with-data-staging:
	-docker rmi --force \
		$(DOCKER_IMAGE_NAME)-with-data-staging:$(GIT_VERSION) \
		$(DOCKER_IMAGE_NAME)-with-data-staging

.PHONY: clean
clean: docker-rmi-for-build docker-rmi-for-build-with-data docker-rmi-for-build-with-data-staging

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------

.PHONY: help
help:
	@echo "List of make targets:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$' | xargs
