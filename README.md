# Distributed Dataset Repository

## Components

- **client** - Client application
- **master** - Master server
- **stserver** - Storage server

## Building

Build all components:
```bash
make build
```

Build individual components:
```bash
make client
make master
make stserver
```

## Proto File Generation

When you modify any `.proto` files, regenerate the Go code with:

```bash
make proto
```

This will regenerate all proto files in:
- `api/chunktransfer/chunk_transfer.proto`
- `api/storage/storage.proto`

**Note:** Always run `make proto` after modifying proto files before building or committing changes.

## Running

```bash
./bin/client
./bin/master
./bin/stserver
```

## Project Structure

```
DDR/
├── cmd/              # Main applications
├── internal/         # Private application code
├── pkg/              # Public library code
└── api/              # API definitions and protocols
    ├── chunktransfer/  # Chunk transfer service proto
    └── storage/        # Storage service proto
```

## Prerequisites

- Go 1.21 or later
- protoc (Protocol Buffer compiler)
- protoc-gen-go
- protoc-gen-go-grpc

To install proto tools:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

