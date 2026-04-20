VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.buildDate=$(BUILD_DATE)

.PHONY: build test lint run clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/socks5lb ./cmd/socks5lb

test:
	go test -race -count=1 ./...

lint:
	go vet ./...

run: build
	./bin/socks5lb -listen 127.0.0.1:1080 -admin-addr 127.0.0.1:9090 \
		-upstream 127.0.0.1:1081 -log-level debug

clean:
	rm -rf bin
