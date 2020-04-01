GOCMD=go
TESTCMD=ginkgo -v -r --trace
LINTCMD=golangci-lint run
GOMOD=$(GOCMD) mod
GOCLEAN=$(GOCMD) clean

DOCKERCMD=docker

ROOT := $$(git rev-parse --show-toplevel)

.PHONY: clean
clean:
				$(GOCLEAN)

.PHONY: lint
lint: 
				$(LINTCMD)

.PHONY: test
test: 
				$(TESTCMD) 


