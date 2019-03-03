GIT_REVISION := $(shell git describe --always)

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOPATH ?= $(shell go env GOPATH)

BIN_NAME := v3iocli-$(GIT_REVISION)-$(GOOS)-$(GOARCH)

.PHONY: build
build:
	CGO_ENABLED=0 go build -v -o "$(PWD)/$(BIN_NAME)" ./cmd/v3iocli
	rm -f v3iocli
	ln -s "$(PWD)/$(BIN_NAME)" v3iocli
