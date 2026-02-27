.PHONY: build clean client master stserver all
.PHONY: run-client run-master run-stserver
.PHONY: win-client win-master win-stserver win-build
.PHONY: run-win-client run-win-master run-win-stserver win-clean

# Build all binaries
all: build

build: client master stserver

# Build individual components (Linux/macOS)
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
		api/metastorage/storage.proto
	@echo "Proto generation complete!"

# Clean build artifacts (Linux/macOS)
clean:
	@echo "Cleaning..."
	@rm -rf bin/

# Run components (Linux/macOS)
run-client: client
	@./bin/client

run-master: master
	@./bin/master

run-stserver: stserver
	@./bin/stserver

# Build individual components (Windows)
win-build: win-client win-master win-stserver

win-client:
	@echo "Building client..."
	@go build -o bin/client.exe ./cmd/client

win-master:
	@echo "Building master..."
	@go build -o bin/master.exe ./cmd/master

win-stserver:
	@echo "Building stserver..."
	@go build -o bin/stserver.exe ./cmd/stserver

# Clean build artifacts (Windows)
win-clean:
	@echo "Cleaning..."
	@if exist bin rd /s /q bin

# Run components (Windows)
run-win-client: win-client
	@.\bin\client.exe

run-win-master: win-master
	@.\bin\master.exe

run-win-stserver: win-stserver
	@.\bin\stserver.exe

