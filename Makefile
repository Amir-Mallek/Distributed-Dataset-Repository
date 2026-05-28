.PHONY: all build client master stserver
.PHONY: proto proto-generate proto-clean
.PHONY: run-client run-master run-stserver
.PHONY: test test-v vet fmt tidy deps
.PHONY: win-build win-client win-master win-stserver win-clean win-proto-clean
.PHONY: run-win-client run-win-master run-win-stserver
.PHONY: help clean

# ── Default ────────────────────────────────────────────────────────────────────
all: build

# ── Build  (Linux / macOS) ─────────────────────────────────────────────────────
build: client master stserver

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

# ── Build  (Windows) ───────────────────────────────────────────────────────────
win-build: win-client win-master win-stserver

win-client:
	@echo "Building client..."
	@if not exist bin mkdir bin
	@go build -o bin\client.exe ./cmd/client

win-master:
	@echo "Building master..."
	@if not exist bin mkdir bin
	@go build -o bin\master.exe ./cmd/master

win-stserver:
	@echo "Building stserver..."
	@if not exist bin mkdir bin
	@go build -o bin\stserver.exe ./cmd/stserver

# ── Proto ──────────────────────────────────────────────────────────────────────
proto: proto-generate

proto-generate:
	@echo "Generating proto files..."
	@protoc \
		--proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/chunktransfer/chunk_transfer.proto \
		api/metastorage/storage.proto \
		api/chunkmap/chunkmap.proto \
		api/master/master.proto
	@echo "Proto generation complete!"

# Remove all generated .pb.go files (Linux / macOS)
proto-clean:
	@echo "Removing generated proto files..."
	@find api -name "*.pb.go" -delete
	@echo "Done."

# Remove all generated .pb.go files (Windows)
win-proto-clean:
	@echo "Removing generated proto files..."
	@del /s /q api\*.pb.go 2>nul
	@echo Done.

# ── Dependencies ───────────────────────────────────────────────────────────────
tidy:
	@echo "Tidying modules..."
	@go mod tidy

deps:
	@echo "Downloading dependencies..."
	@go mod download

# ── Code quality (same on both platforms) ──────────────────────────────────────
fmt:
	@echo "Formatting code..."
	@go fmt ./...

vet:
	@echo "Running go vet..."
	@go vet ./...

# ── Tests (same on both platforms) ─────────────────────────────────────────────
test:
	@echo "Running tests..."
	@go test ./...

test-v:
	@echo "Running tests (verbose)..."
	@go test -v ./...

# ── Run  (Linux / macOS) ───────────────────────────────────────────────────────
# Usage: make run-stserver
run-stserver: stserver
	@./bin/stserver

# Usage: make run-master
run-master: master
	@./bin/master

# Usage: make run-client CLIENT_ID=alice DATASET_ID=ds1 FILE=./myfile.txt
run-client: client
	@./bin/client $(CLIENT_ID) $(DATASET_ID) $(FILE)

# ── Run  (Windows) ─────────────────────────────────────────────────────────────
# Usage: make run-win-stserver
run-win-stserver: win-stserver
	@.\bin\stserver.exe

# Usage: make run-win-master
run-win-master: win-master
	@.\bin\master.exe

# Usage: make run-win-client CLIENT_ID=alice DATASET_ID=ds1 FILE=.\myfile.txt
run-win-client: win-client
	@.\bin\client.exe $(CLIENT_ID) $(DATASET_ID) $(FILE)

# ── Clean ──────────────────────────────────────────────────────────────────────
clean:
	@echo "Cleaning..."
	@rm -rf bin/

win-clean:
	@echo "Cleaning..."
	@if exist bin rd /s /q bin

# ── Help ───────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  PROTO"
	@echo "    proto              Generate all .pb.go files from .proto sources"
	@echo "    proto-clean        Delete generated .pb.go files          (Linux/macOS)"
	@echo "    win-proto-clean    Delete generated .pb.go files          (Windows)"
	@echo ""
	@echo "  BUILD"
	@echo "    build              Build all binaries                      (Linux/macOS)"
	@echo "    client             Build client only                       (Linux/macOS)"
	@echo "    master             Build master only                       (Linux/macOS)"
	@echo "    stserver           Build storage server only               (Linux/macOS)"
	@echo "    win-build          Build all binaries                      (Windows)"
	@echo "    win-client         Build client only                       (Windows)"
	@echo "    win-master         Build master only                       (Windows)"
	@echo "    win-stserver       Build storage server only               (Windows)"
	@echo ""
	@echo "  RUN"
	@echo "    run-stserver       Start storage server                    (Linux/macOS)"
	@echo "    run-master         Start master server                     (Linux/macOS)"
	@echo "    run-client         Upload file  CLIENT_ID= DATASET_ID= FILE=  (Linux/macOS)"
	@echo "    run-win-stserver   Start storage server                    (Windows)"
	@echo "    run-win-master     Start master server                     (Windows)"
	@echo "    run-win-client     Upload file  CLIENT_ID= DATASET_ID= FILE=  (Windows)"
	@echo ""
	@echo "  QUALITY  (same on both platforms)"
	@echo "    fmt                Format all Go source files"
	@echo "    vet                Run go vet on all packages"
	@echo "    test               Run all tests"
	@echo "    test-v             Run all tests with verbose output"
	@echo ""
	@echo "  DEPS  (same on both platforms)"
	@echo "    tidy               Run go mod tidy"
	@echo "    deps               Download all module dependencies"
	@echo ""
	@echo "  CLEAN"
	@echo "    clean              Remove bin/                             (Linux/macOS)"
	@echo "    win-clean          Remove bin/                             (Windows)"
	@echo ""

