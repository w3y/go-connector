.PHONY: all
all: proto_generate proto_clean clean

# The binary to build, path to dir under cmd
BIN := 
# This repo's root import path (under GOPATH).
CONNECTORREPO := github.com/w3y/go-connector

# Check if current dir is git repo?
ifeq ($(wildcard $(if $(shell git rev-parse --show-toplevel 2>/dev/null),$(shell git rev-parse --show-toplevel)/,).git),)
	# This version-strategy uses a manual value to set the version string
	VERSION := v1.0-snapshot
else
	# This version-strategy uses git tags to set the version string
	VERSION := $(shell git describe --tags --always --dirty="-dev")
endif

# check os and arch
GOOS	= linux
GOARCH	= amd64

UNAME	:= $(shell uname)
OS		:= $(shell echo $(UNAME) |tr '[:upper:]' '[:lower:]')
ARCH	:= $(shell uname -m)
ifeq ($(OS), darwin)
	GOOS = darwin
endif

ifneq ($(ARCH), x86_64)
	GOARCH = 386
endif

# Replace backslashes with forward slashes for use on Windows.
# Make is !@#$ing weird.
E		:=
BSLASH 	:= \$E
FSLASH 	:= /

# Directories
WD			:= $(subst $(BSLASH),$(FSLASH),$(shell pwd))
MD			:= $(subst $(BSLASH),$(FSLASH),$(shell dirname "$(realpath $(lastword $(MAKEFILE_LIST)))"))
DISTDIR		:= $(WD)/dist
CONFDIR		:= $(WD)/conf

# Parameters
LDFLAGS		:= -X main.version=$(VERSION)

# Space separated patterns of packages to skip in list, test, format.
IGNORED_PACKAGES := /vendor/

# generate protobuf
proto_generate:
	@for d in ./proto/*; do \
		protoc --proto_path=. --proto_path=$(GOPATH)/src \
			--go_out=plugins=grpc:$(GOPATH)/src $$d; \
		echo "compiled: $$d"; \
	done

proto_clean:
	@rm -f $(GOPATH)/src/$(CONNECTORREPO)/pb/*.pb.go

# cd into the GOPATH to workaround ./... not following symlinks
_allpackages = $(shell ( go list ./... 2>&1 1>&3 | \
	grep -v -e "^$$" $(addprefix -e ,$(IGNORED_PACKAGES)) 1>&2 ) 3>&1 | \
	grep -v -e "^$$" $(addprefix -e ,$(IGNORED_PACKAGES)))

# memoize allpackages, so that it's executed only once and only if used
allpackages = $(if $(__allpackages),,$(eval __allpackages := $$(_allpackages)))$(__allpackages)