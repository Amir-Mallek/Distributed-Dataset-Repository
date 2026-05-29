# Distributed Dataset Repository

A small replicated dataset store built around a single master and multiple storage servers.

## Components

- `client`: uploads datasets and reads chunk ranges
- `master`: assigns chunk IDs and replica sets, stores dataset/server metadata
- `stserver`: stores chunk data and forwards writes to the next replica

## Build

Build all binaries:

```bash
make build
```

Build individual binaries:

```bash
make client
make master
make stserver
make genfile
```

## Run With Docker Compose

Start the full stack:

```bash
make compose-up
```

This starts:

- `master` on `localhost:50051`
- `storage1` on `localhost:50052`
- `storage2` on `localhost:50053`
- `storage3` on `localhost:50054`
- `storage4` on `localhost:50055`

Tail logs:

```bash
make compose-logs
```

Stop everything:

```bash
make compose-down
```

## Client Usage

Upload a file as a dataset:

```bash
./bin/client upload ./path/to/file
```

The upload command:

- creates a dataset record on the master
- requests a chunk ID and replica set for each chunk
- uploads each chunk to the first replica
- forwards the chunk across the replica chain

Read a byte range from a chunk directly from a storage server:

```bash
./bin/client read-chunk --server localhost:50052 --chunk-id <chunk-id> --start 0 --end 16 --out download.bin
```

## End-to-End Smoke Test

The Makefile exposes explicit steps so you can run the flow in order:

```bash
make e2e-up
make e2e-generate SIZE=1048576
make e2e-upload
make e2e-read CHUNK_ID=<chunk-id> SERVER=localhost:50052
make e2e-clean
```

What each step does:

- `e2e-up`: starts the compose stack and waits briefly for startup
- `e2e-generate`: creates `artifacts/test.bin` with the requested size
- `e2e-upload`: uploads `artifacts/test.bin` and writes the log to `artifacts/upload.log`
- `e2e-read`: reads back the uploaded chunk from a storage server
- `e2e-clean`: stops compose and removes generated artifacts

If you want the chunk ID automatically, run `make e2e-upload` first and copy the `chunk_id=...` line from `artifacts/upload.log`.

## Generate a Test File

Create a file of a specific size:

```bash
go run ./tools/genfile --out artifacts/test.bin --size 1048576
```

Defaults:

- output: `artifacts/sample.bin`
- size: `1048576` bytes

## Proto Regeneration

When `.proto` files change, regenerate Go bindings:

```bash
make proto
```

## Notes

- Generated files go under `artifacts/` and are ignored by git.
- The client CLI currently exposes `upload` and `read-chunk` only.
- Master and storage registration use the compose service names inside Docker and local mapped ports from the host.