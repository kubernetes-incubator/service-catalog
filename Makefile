# Copyright 2016 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

all: build test verify

# Define some constants
#######################
ROOT           = $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
BINDIR        ?= bin
COVERAGE      ?= $(CURDIR)/coverage.html
SC_PKG         = github.com/kubernetes-incubator/service-catalog
TOP_SRC_DIRS   = cmd contrib pkg
SRC_DIRS       = $(shell sh -c "find $(TOP_SRC_DIRS) -name \\*.go \
                   -exec dirname {} \\; | sort | uniq")
TEST_DIRS      = $(shell sh -c "find $(TOP_SRC_DIRS) -name \\*_test.go \
                   -exec dirname {} \\; | sort | uniq")
VERSION       ?= $(shell git describe --tags --always --abbrev=7 --dirty)
ifeq ($(shell uname -s),Darwin)
STAT           = stat -f '%c %N'
else
STAT           = stat -c '%Y %n'
endif
NEWEST_GO_FILE = $(shell find $(SRC_DIRS) -name \*.go -exec $(STAT) {} \; \
                   | sort -r | head -n 1 | sed "s/.* //")
TYPES_FILES    = $(shell find pkg/apis -name types.go)
GO_VERSION     = 1.7.3
GO_BUILD       = env GOOS=linux GOARCH=amd64 go build -i -v \
                   -ldflags "-X $(SC_PKG)/pkg.VERSION=$(VERSION)"
BASE_PATH      = $(ROOT:/src/github.com/kubernetes-incubator/service-catalog/=)
export GOPATH  = $(BASE_PATH):$(ROOT)/vendor

ifneq ($(origin DOCKER),undefined)
  # If DOCKER is defined then define the full docker cmd line we want to use
  DOCKER_FLAG  = DOCKER=1
  DOCKER_CMD   = docker run --rm -ti -v $(PWD):/go/src/$(SC_PKG) scbuildimage
  # Setting scBuildImageTarget will force the Docker image to be built
  # in the .init rule
  scBuildImageTarget=.scBuildImage
endif

# This section builds the output binaries.
# Some will have dedicated targets to make it easier to type, for example
# "apiserver" instead of "bin/apiserver".
#########################################################################
build: .init .generate_files \
       $(BINDIR)/controller $(BINDIR)/apiserver \
       $(BINDIR)/registry $(BINDIR)/k8s-broker $(BINDIR)/user-broker

controller: $(BINDIR)/controller
$(BINDIR)/controller: .init cmd/controller \
	  $(shell find cmd/controller -type f)
	$(DOCKER_CMD) $(GO_BUILD) -o $@ $(SC_PKG)/cmd/controller

registry: $(BINDIR)/registry
$(BINDIR)/registry: .init contrib/cmd/registry \
	  $(shell find contrib/cmd/registry -type f)
	$(DOCKER_CMD) $(GO_BUILD) -o $@ $(SC_PKG)/contrib/cmd/registry

k8s-broker: $(BINDIR)/k8s-broker
$(BINDIR)/k8s-broker: .init contrib/cmd/k8s-broker \
	  $(shell find contrib/cmd/k8s-broker -type f)
	$(DOCKER_CMD) $(GO_BUILD) -o $@ $(SC_PKG)/contrib/cmd/k8s-broker

user-broker: $(BINDIR)/user-broker
$(BINDIR)/user-broker: .init contrib/cmd/user-broker \
	  $(shell find contrib/cmd/user-broker -type f)
	$(DOCKER_CMD) $(GO_BUILD) -o $@ $(SC_PKG)/contrib/cmd/user-broker

# We'll rebuild apiserver if any go file has changed (ie. NEWEST_GO_FILE)
apiserver: $(BINDIR)/apiserver
$(BINDIR)/apiserver: .init .generate_files cmd/apiserver $(NEWEST_GO_FILE)
	$(DOCKER_CMD) $(GO_BUILD) -o $@ $(SC_PKG)/cmd/apiserver

# This section contains the code generation stuff
#################################################
.generate_exes: $(BINDIR)/defaulter-gen $(BINDIR)/deepcopy-gen
	touch $@

$(BINDIR)/defaulter-gen: .init cmd/libs/go2idl/defaulter-gen
	$(DOCKER_CMD) go build -o $@ $(SC_PKG)/cmd/libs/go2idl/defaulter-gen

$(BINDIR)/deepcopy-gen: .init cmd/libs/go2idl/deepcopy-gen
	$(DOCKER_CMD) go build -o $@ $(SC_PKG)/cmd/libs/go2idl/deepcopy-gen

# Regenerate all files if the gen exes changed or any "types.go" files changed
.generate_files: .init .generate_exes $(TYPES_FILES)
	$(DOCKER_CMD) $(BINDIR)/defaulter-gen --v 1 --logtostderr \
	  -i $(SC_PKG)/pkg/apis/servicecatalog,$(SC_PKG)/pkg/apis/servicecatalog/v1alpha1 \
	  --extra-peer-dirs $(SC_PKG)/pkg/apis/servicecatalog,$(SC_PKG)/pkg/apis/servicecatalog/v1alpha1 \
	  -O zz_generated.defaults
	$(DOCKER_CMD) $(BINDIR)/deepcopy-gen --v 1 --logtostderr \
	  -i $(SC_PKG)/pkg/apis/servicecatalog,$(SC_PKG)/pkg/apis/servicecatalog/v1alpha1 \
	  --bounding-dirs github.com/kubernetes-incubator/service-catalog \
	  -O zz_generated.deepcopy
	  touch $@

# Some prereq stuff
###################
.init: $(scBuildImageTarget) glide.yaml
	$(DOCKER_CMD) glide install --strip-vendor
	touch $@

.scBuildImage: build/build-image/Dockerfile
	sed "s/GO_VERSION/$(GO_VERSION)/g" < build/build-image/Dockerfile | \
	  docker build -t scbuildimage -
	touch $@

# Util targets
##############
verify: .init .generate_files
	@echo Running gofmt:
	@$(DOCKER_CMD) gofmt -l -s $(TOP_SRC_DIRS) > .out 2>&1 || true
	@bash -c '[ "`cat .out`" == "" ] || \
	  (echo -e "\n*** Please 'gofmt' the following:" ; cat .out ; echo ; false)'
	@rm .out
	@echo Running golint and go vet:
	# Exclude the generated (zz) files for now
	@# The following command echo's the "for" loop to stdout so that it can
	@# be piped to the "sh" cmd running in the container. This allows the
	@# "for" to be executed in the container and not on the host. Which means
	@# we have just one container for everything and not one container per
	@# file.  The $(subst) removes the "-t" flag from the Docker cmd.
	@echo for i in \`find $(TOP_SRC_DIRS) -name \*.go \| grep -v zz\`\; do \
	  golint --set_exit_status \$$i \; \
	  go vet \$$i \; \
	done | $(subst -ti,-i,$(DOCKER_CMD)) sh -e
	@echo Running repo-infra verify scripts
	$(DOCKER_CMD) vendor/github.com/kubernetes/repo-infra/verify/verify-boilerplate.sh --rootdir=. | grep -v zz_generated > .out 2>&1 || true
	@bash -c '[ "`cat .out`" == "" ] || (cat .out ; false)'
	@rm .out

format: .init
	$(DOCKER_CMD) gofmt -w -s $(TOP_SRC_DIRS)

coverage: .init
	$(DOCKER_CMD) hack/coverage.sh --html "$(COVERAGE)" \
	  $(addprefix ./,$(TEST_DIRS))

test: test-unit test-e2e

test-unit: .init
	@echo Running tests:
	@for i in $(addprefix $(SC_PKG)/,$(TEST_DIRS)); do \
	  $(DOCKER_CMD) go test $$i || exit $$? ; \
	done

test-e2e: .init images
	@echo Running e2e tests:
	contrib/bin/walkthru

clean:
	rm -rf $(BINDIR)
	rm -f .init .scBuildImage .generate_files .generate_exes
	rm -f $(COVERAGE)
	find $(TOP_SRC_DIRS) -name zz_generated* -exec rm {} \;
	docker rmi -f scbuildimage > /dev/null 2>&1 || true

# Building Docker Images for our executables
############################################
images: registry-image k8s-broker-image user-broker-image controller-image

registry-image: contrib/build/registry/Dockerfile $(BINDIR)/registry
	mkdir -p contrib/build/registry/tmp
	cp $(BINDIR)/registry contrib/build/registry/tmp
	cp contrib/pkg/registry/data/charts/*.json contrib/build/registry/tmp
	docker build -t registry:$(VERSION) contrib/build/registry
	rm -rf contrib/build/registry/tmp

k8s-broker-image: contrib/build/k8s-broker/Dockerfile $(BINDIR)/k8s-broker
	mkdir -p contrib/build/k8s-broker/tmp
	cp $(BINDIR)/k8s-broker contrib/build/k8s-broker/tmp
	docker build -t k8s-broker:$(VERSION) contrib/build/k8s-broker
	rm -rf contrib/build/k8s-broker/tmp

user-broker-image: contrib/build/user-broker/Dockerfile $(BINDIR)/user-broker
	mkdir -p contrib/build/user-broker/tmp
	cp $(BINDIR)/user-broker contrib/build/user-broker/tmp
	docker build -t user-broker:$(VERSION) contrib/build/user-broker
	rm -rf contrib/build/user-broker/tmp

controller-image: build/controller/Dockerfile $(BINDIR)/controller
	mkdir -p build/controller/tmp
	cp $(BINDIR)/controller build/controller/tmp
	docker build -t controller:$(VERSION) build/controller
	rm -rf build/controller/tmp

# Push our Docker Images to a registry
######################################
push: registry-push k8s-broker-push user-broker-push controller-push

registry-push: registry-image
	[ ! -z "$(REGISTRY)" ] || (echo Set your REGISTRY env var first ; exit 1)
	docker tag registry:$(VERSION) $(REGISTRY)/registry:$(VERSION)
	docker push $(REGISTRY)/registry:$(VERSION)

k8s-broker-push: k8s-broker-image
	[ ! -z "$(REGISTRY)" ] || (echo Set your REGISTRY env var first ; exit 1)
	docker tag k8s-broker:$(VERSION) $(REGISTRY)/k8s-broker:$(VERSION)
	docker push $(REGISTRY)/k8s-broker:$(VERSION)

user-broker-push: user-broker-image
	[ ! -z "$(REGISTRY)" ] || (echo Set your REGISTRY env var first ; exit 1)
	docker tag user-broker:$(VERSION) $(REGISTRY)/user-broker:$(VERSION)
	docker push $(REGISTRY)/user-broker:$(VERSION)

controller-push: controller-image
	[ ! -z "$(REGISTRY)" ] || (echo Set your REGISTRY env var first ; exit 1)
	docker tag controller:$(VERSION) $(REGISTRY)/controller:$(VERSION)
	docker push $(REGISTRY)/controller:$(VERSION)
