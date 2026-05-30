# Fault-Tolerance E2E

This script exercises two failure cases against the docker-compose stack:

1. Stop one storage container and wait for master heartbeat timeout, then verify a new replica appears on the spare storage node.
2. Corrupt a stored chunk on a storage server, read it to trigger checksum failure, and verify the master re-replicates to the spare node.

## Prerequisites

- Docker + docker compose
- Go toolchain (for `go run ./cmd/client`)

## Run

From the repo root:

```bash
make e2e-fault
```

Artifacts are written under `artifacts/`.

If you want to run the script directly:

```bash
bash tools/e2e/fault_tolerance.sh
```

## Notes

- The script assumes the default compose services and ports from `docker-compose.yml`.
- Reads use a 1MB aligned range so checksum verification is enforced.
