# Try to get the commit hash from 1) git 2) the VERSION file 3) fallback.
LAST_COMMIT := $(or $(shell git rev-parse --short HEAD 2> /dev/null),$(shell head -n 1 VERSION | grep -oP -m 1 "^[a-z0-9]+$$"),"")

# Try to get the semver from 1) git 2) the VERSION file 3) fallback.
VERSION := $(or $(LISTMONK_VERSION),$(shell git describe --tags --abbrev=0 2> /dev/null),$(shell grep -oP 'tag: \Kv\d+\.\d+\.\d+(-[a-zA-Z0-9.-]+)?' VERSION),"v0.0.0")
BUILDSTR := ${VERSION} (\#${LAST_COMMIT} $(shell date -u +"%Y-%m-%dT%H:%M:%S%z"))

BIN := kaf-relay

.PHONY: build
build: $(BIN)

$(BIN): $(shell find . -type f -name "*.go") go.mod go.sum
	CGO_ENABLED=0 go build -o ${BIN} --ldflags="-X 'main.buildString=${BUILDSTR}'"

.PHONY: clean
clean:
	rm -rf ${BIN}
