.PHONY: build test clean install

build:
@echo "Building opensearch-init" && go build -o opensearch-init

test:
@go vet ./...

clean:
@rm -f opensearch-init

install:
@go install
