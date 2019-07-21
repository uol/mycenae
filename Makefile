#
# Mycenae project
#
SHELL := /bin/bash
COVER := /tmp/mycenae.cover

#
# Targets
#
default: build
build install:
	@echo " === Running: $@ === "
	@GOARCH=amd64
	@go "$@" -a -v -o mycenae
	@echo
test $(COVER): $(wildcard *.go)
	@go test -v -cover -coverprofile="$(COVER)"
cover: $(COVER) $(wildcard *.go)
	@go tool cover -html="$(COVER)"

run: build
	@"./mycenae" -config=config.toml

clean:
	@go clean

rpm-build:
	@echo " === Generating RPM === "
	@rpmbuild -bb ./rpm/mycenae.spec
	@echo " === RPM Generated === "
