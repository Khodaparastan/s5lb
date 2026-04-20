VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.buildDate=$(BUILD_DATE)

.PHONY: build test lint run clean tidy

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/socks5lb ./cmd/socks5lb

test:
	go test -race -count=1 ./...

lint:
	go vet ./...

tidy:
	go mod tidy

run: build
	./bin/socks5lb -config config.example.yaml -log-level debug

clean:
	rm -rf bin
