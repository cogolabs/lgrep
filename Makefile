VERSION ?= $(shell git describe --dirty --tags)
GIT_REF := $(shell git describe --always)
RELEASE_DIR := $(shell pwd)/release/$(VERSION)
BUILD_FLAGS := -ldflags "-X main.Version=$(VERSION) -X main.Commit=$(GIT_REF)"

.PHONY: release test


release: build-linux build-osx
	tar -C $(RELEASE_DIR)/linux-x86_64 -zcf $(RELEASE_DIR)/lgrep-linux-x86_64.tar.gz \
		lgrep
	tar -C $(RELEASE_DIR)/macos-x86_64 -zcf $(RELEASE_DIR)/lgrep-macos-x86_64.tar.gz \
		lgrep

build-linux:
	mkdir -p $(RELEASE_DIR)/linux-x86_64
	GOOS=linux  go build -o $(RELEASE_DIR)/linux-x86_64/lgrep $(BUILD_FLAGS) \
		./cmd/lgrep

build-osx:
	mkdir -p $(RELEASE_DIR)/macos-x86_64
	GOOS=darwin go build -o $(RELEASE_DIR)/macos-x86_64/lgrep $(BUILD_FLAGS) \
		./cmd/lgrep
clean:
	rm -rf $(dir $(RELEASE_DIR))*

install:
	go install $(BUILD_FLAGS) ./cmd/lgrep

test:
	go test . ./test ./cmd/lgrep
