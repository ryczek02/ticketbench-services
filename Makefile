SERVICES := bench-runner store-postgres store-mariadb store-mongo store-valkey
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE     ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG      := github.com/ryczek02/ticketbench-contracts/pkg/version
LDFLAGS  := -s -w -X $(PKG).Version=$(VERSION) -X $(PKG).Commit=$(COMMIT) -X $(PKG).Date=$(DATE)
IMAGE    ?= ghcr.io/ryczek02/ticketbench

.PHONY: build test test-unit bench images lint clean

build: $(addprefix bin/,$(SERVICES))

bin/%: $(shell find . -name '*.go' -o -name '*.sql')
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $@ ./cmd/$*

test-unit:
	go test -short -race ./...

# Conformance tests start PostgreSQL, MariaDB, MongoDB and Valkey in Docker.
test:
	go test -race -timeout 20m ./...

bench:
	go test -run '^$$' -bench . -benchmem ./...

images:
	for s in $(SERVICES); do \
		docker build --build-arg SERVICE=$$s --build-arg VERSION=$(VERSION) \
			--build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t $(IMAGE)-$$s:$(VERSION) . || exit 1; \
	done

lint:
	go vet ./...
	golangci-lint run

clean:
	rm -rf bin dist
