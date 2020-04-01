GOCMD=go
TESTCMD=ginkgo -v -r --trace
LINTCMD=golangci-lint run
GOMOD=$(GOCMD) mod
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
BINARY_NAME=gohub-nats-arangodb-sapi

DOCKERCMD=docker

ROOT := $$(git rev-parse --show-toplevel)

all: lint build
.PHONY: all

.PHONY: clean
clean:
				$(GOCLEAN)

.PHONY: lint
lint: 
				$(LINTCMD)

.PHONY: test
test: 
				$(TESTCMD) 

.PHONY: build
build: 
				$(GOBUILD) -o $(ROOT)/bin/$(BINARY_NAME) -v

.PHONY: docker
docker:
				$(DOCKERCMD) build -t codelity-co/gohub-nats-arangodb-sapi .
