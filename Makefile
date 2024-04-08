# Image and tag can be overridden via environment variables.
DOCKER_USERNAME ?= synthao
IMAGE_NAME ?= notifier
TAG ?= latest

# Name of the Docker image.
IMAGE := ${DOCKER_USERNAME}/${IMAGE_NAME}:${TAG}

.PHONY: all build push

all: build push

build:
	@echo "Building Docker image ${IMAGE}"
	@docker build -t ${IMAGE} .

push:
	@echo "Pushing Docker image ${IMAGE}"
	@docker push ${IMAGE}