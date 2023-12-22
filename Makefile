# Git version for injecting into Go bins.
LAST_COMMIT := $(shell git rev-parse --short HEAD)
LAST_COMMIT_DATE := $(shell git show -s --format=%ci ${LAST_COMMIT})
VERSION := $(shell git describe --tags)
BUILDSTR := ${VERSION} (Commit: ${LAST_COMMIT_DATE} (${LAST_COMMIT}), Build: $(shell date +"%Y-%m-%d% %H:%M:%S %z"))

BIN := kaf-relay.bin
DIST := dist

.PHONY: dist
dist:
	mkdir -p ${DIST}
	CGO_ENABLED=1 go build -o ${BIN} --ldflags="-X 'main.buildString=${BUILDSTR}'"
	cp ${BIN} ${DIST}

.PHONY: clean
clean:
	rm -rf ${DIST}