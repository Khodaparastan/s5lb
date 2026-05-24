SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

GO ?= go
APP_NAME := socks5lb
CMD_DIR := ./cmd/socks5lb
BIN_DIR := $(CURDIR)/bin
DIST_DIR := $(CURDIR)/dist
GOFILES := ./cmd ./internal
PKG := ./...
PLATFORMS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64

VERSION    ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.buildDate=$(BUILD_DATE)

CGO_ENABLED ?= 0
export CGO_ENABLED
GO_BUILD_FLAGS := -mod=readonly

.PHONY: help build install release build-linux build-darwin build-windows clean deps fmt fmt-check lint test vet check tidy mod-verify run dev

help:
	@printf '%s\n' \
		'Targets:' \
		'  make build        Build a stripped, reproducible binary into ./bin' \
		'  make install      Install the binary into your Go bin directory' \
		'  make release      Build versioned artifacts for common OS/ARCH pairs' \
		'  make test         Run the full test suite with the race detector' \
		'  make check        Run format, vet, and tests' \
		'  make run          Build and start the service with config.yaml' \
		'  make dev          Build and start the service with config.example.yaml' \
		'  make clean        Remove build artifacts'

build: $(BIN_DIR)/$(APP_NAME)

$(BIN_DIR)/$(APP_NAME):
	@mkdir -p "$(BIN_DIR)"
	$(GO) build $(GO_BUILD_FLAGS) -trimpath -ldflags "$(LDFLAGS)" -o "$@" $(CMD_DIR)

install:
	$(GO) install $(GO_BUILD_FLAGS) -trimpath -ldflags "$(LDFLAGS)" $(CMD_DIR)

release:
	@mkdir -p "$(DIST_DIR)"
	@set -euo pipefail; \
	for target in $(PLATFORMS); do \
		os="$${target%/*}"; \
		arch="$${target#*/}"; \
		out="$(DIST_DIR)/$(APP_NAME)-$(VERSION)-$${os}-$${arch}"; \
		ext=""; \
		if [[ "$$os" == windows ]]; then ext=".exe"; fi; \
		echo "building $${out}$${ext}"; \
		GOOS="$$os" GOARCH="$$arch" $(GO) build $(GO_BUILD_FLAGS) -trimpath -ldflags "$(LDFLAGS)" -o "$${out}$${ext}" $(CMD_DIR); \
	done

build-linux:
	@mkdir -p "$(DIST_DIR)"
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -trimpath -ldflags "$(LDFLAGS)" -o "$(DIST_DIR)/$(APP_NAME)-$(VERSION)-linux-amd64" $(CMD_DIR)

build-darwin:
	@mkdir -p "$(DIST_DIR)"
	GOOS=darwin GOARCH=arm64 $(GO) build $(GO_BUILD_FLAGS) -trimpath -ldflags "$(LDFLAGS)" -o "$(DIST_DIR)/$(APP_NAME)-$(VERSION)-darwin-arm64" $(CMD_DIR)

build-windows:
	@mkdir -p "$(DIST_DIR)"
	GOOS=windows GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -trimpath -ldflags "$(LDFLAGS)" -o "$(DIST_DIR)/$(APP_NAME)-$(VERSION)-windows-amd64.exe" $(CMD_DIR)

fmt:
	$(GO) fmt $(PKG)

fmt-check:
	@files="$$(gofmt -l $(GOFILES))"; \
	test -z "$$files" || { \
		echo "run make fmt"; \
		printf '%s\n' "$$files"; \
		exit 1; \
	}

vet:
	$(GO) vet $(PKG)

lint: vet

test:
	$(GO) test $(GO_BUILD_FLAGS) -race -count=1 $(PKG)

check: fmt-check vet test

deps:
	$(GO) mod download

tidy:
	$(GO) mod tidy

mod-verify:
	$(GO) mod verify

run: build
	"$(BIN_DIR)/$(APP_NAME)" -config config.yaml -log-level info -log-format json

dev: build
	"$(BIN_DIR)/$(APP_NAME)" -config config.example.yaml -log-level debug -log-format text

clean:
	rm -rf "$(BIN_DIR)" "$(DIST_DIR)"
