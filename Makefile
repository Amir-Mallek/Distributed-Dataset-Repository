.PHONY: build clean client master stserver all proto proto-generate

# Build all binaries
all: build

build: client master stserver

# Build individual components
client:
	@echo "Building client..."
	@mkdir -p bin
	@go build -o bin/client ./cmd/client

master:
	@echo "Building master..."
	@mkdir -p bin
	@go build -o bin/master ./cmd/master

stserver:
	@echo "Building stserver..."
	@mkdir -p bin
	@go build -o bin/stserver ./cmd/stserver

# Proto file generation
proto: proto-generate

proto-generate:
	@echo "Generating proto files..."
	@protoc \
		--go_out=. \
		--go-grpc_out=. \
		--proto_path=. \
		api/chunktransfer/chunk_transfer.proto \
		api/storage/storage.proto
	@echo "Proto generation complete!"

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf bin/

# Run components (for development)
run-client: client
	@./bin/client

run-master: master
	@./bin/master

run-stserver: stserver
	@./bin/stserver

