# DDR

A distributed system written in Go.

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
```

