#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACTS_DIR="$ROOT_DIR/artifacts"
UPLOAD_LOG="$ARTIFACTS_DIR/upload.log"

ALL_ADDRS=("storage1:50051" "storage2:50051" "storage3:50051" "storage4:50051")

addr_to_index() {
  local addr="$1"
  local name="${addr%%:*}"
  echo "${name#storage}"
}

addr_to_container() {
  local idx
  idx="$(addr_to_index "$1")"
  echo "ddr-storage-$idx"
}

addr_to_host() {
  local idx
  idx="$(addr_to_index "$1")"
  echo "localhost:$((50051 + idx))"
}

parse_dataset_id() {
  grep -m1 -oE 'id=[0-9a-f-]+' "$UPLOAD_LOG" | cut -d= -f2
}

parse_chunk_id() {
  local cid
  cid="$(grep -m1 -oE 'chunk_id=[0-9a-f-]+' "$UPLOAD_LOG" | cut -d= -f2 || true)"
  if [ -z "$cid" ]; then
    cid="$(grep -m1 -oE 'chunkID=[0-9a-f-]+' "$UPLOAD_LOG" | cut -d= -f2 || true)"
  fi
  echo "$cid"
}

parse_replicas() {
  local line
  line="$(grep -m1 -oE 'replica \[[^]]+\]' "$UPLOAD_LOG" | sed -E 's/^.*replica \[//; s/\]$//')"
  echo "$line"
}

wait_for_copy() {
  local container="$1"
  local file_path="$2"
  local timeout_secs="$3"
  local start
  start="$(date +%s)"

  while true; do
    local now
    now="$(date +%s)"
    if [ $((now - start)) -ge "$timeout_secs" ]; then
      return 1
    fi
    local tmpdir
    tmpdir="$(mktemp -d)"
    if docker cp "${container}:${file_path}" "$tmpdir/chunk.bin" >/dev/null 2>&1; then
      rm -rf "$tmpdir"
      return 0
    fi
    rm -rf "$tmpdir"
    sleep 2
  done
}

copy_out() {
  local container="$1"
  local file_path="$2"
  local out_path="$3"
  mkdir -p "$(dirname "$out_path")"
  docker cp "${container}:${file_path}" "$out_path" >/dev/null
}

corrupt_file() {
  local file_path="$1"
  printf 'CORRUPTED' | dd of="$file_path" bs=1 seek=0 conv=notrunc status=none
}

ensure_artifacts() {
  mkdir -p "$ARTIFACTS_DIR"
}

run_upload() {
  ensure_artifacts
  make -C "$ROOT_DIR" e2e-upload
}

run_generate() {
  ensure_artifacts
  make -C "$ROOT_DIR" e2e-generate
}

run_up() {
  make -C "$ROOT_DIR" e2e-up
}

run_down() {
  make -C "$ROOT_DIR" e2e-down
}

run_read_expect_failure() {
  local server_addr="$1"
  local chunk_id="$2"
  local out_path="$3"

  local output
  local status
  set +e
  output=$(go run "$ROOT_DIR/cmd/client" read-chunk --server "$server_addr" --chunk-id "$chunk_id" --start 0 --end 1048576 --out "$out_path" 2>&1)
  status=$?
  set -e

  if [ "$status" -eq 0 ]; then
    echo "expected read failure but got success"
    return 1
  fi

  echo "read failed as expected: $(echo "$output" | head -n 1)"
}

run_read_expect_success() {
  local server_addr="$1"
  local chunk_id="$2"
  local out_path="$3"

  go run "$ROOT_DIR/cmd/client" read-chunk --server "$server_addr" --chunk-id "$chunk_id" --start 0 --end 1048576 --out "$out_path" >/dev/null
  echo "read succeeded from $server_addr"
}

find_spare_addr() {
  local -a replicas=("$@")
  for addr in "${ALL_ADDRS[@]}"; do
    local found=false
    for r in "${replicas[@]}"; do
      if [ "$r" = "$addr" ]; then
        found=true
        break
      fi
    done
    if [ "$found" = false ]; then
      echo "$addr"
      return 0
    fi
  done
  return 1
}

run_test_server_down() {
  echo "[test1] simulating storage down and re-replication"

  local dataset_id
  dataset_id="$(parse_dataset_id)"
  local chunk_id
  chunk_id="$(parse_chunk_id)"
  local replica_line
  replica_line="$(parse_replicas)"
  if [ -z "$dataset_id" ] || [ -z "$chunk_id" ] || [ -z "$replica_line" ]; then
    echo "failed to parse upload log for test1"
    return 1
  fi

  read -r -a replicas <<< "$replica_line"
  local down_addr="${replicas[0]}"
  local down_container
  down_container="$(addr_to_container "$down_addr")"

  local spare_addr
  spare_addr="$(find_spare_addr "${replicas[@]}")"
  local spare_container
  spare_container="$(addr_to_container "$spare_addr")"

  local file_path="/app/chunks/client-1/${dataset_id}/${chunk_id}"

  docker stop "$down_container" >/dev/null
  sleep 45

  if ! wait_for_copy "$spare_container" "$file_path" 60; then
    echo "replica did not appear on spare storage"
    return 1
  fi

  local source_addr
  source_addr="${replicas[1]}"
  local source_container
  source_container="$(addr_to_container "$source_addr")"

  local src_path="$ARTIFACTS_DIR/src_chunk.bin"
  local dst_path="$ARTIFACTS_DIR/spare_chunk.bin"
  copy_out "$source_container" "$file_path" "$src_path"
  copy_out "$spare_container" "$file_path" "$dst_path"

  local src_hash
  local dst_hash
  src_hash="$(sha256sum "$src_path" | awk '{print $1}')"
  dst_hash="$(sha256sum "$dst_path" | awk '{print $1}')"

  if [ "$src_hash" != "$dst_hash" ]; then
    echo "replica content mismatch after re-replication"
    return 1
  fi

  docker start "$down_container" >/dev/null
  sleep 15
}

run_test_corruption() {
  echo "[test2] corrupting chunk and verifying re-replication"

  run_upload

  local dataset_id
  dataset_id="$(parse_dataset_id)"
  local chunk_id
  chunk_id="$(parse_chunk_id)"
  local replica_line
  replica_line="$(parse_replicas)"
  if [ -z "$dataset_id" ] || [ -z "$chunk_id" ] || [ -z "$replica_line" ]; then
    echo "failed to parse upload log for test2"
    return 1
  fi

  read -r -a replicas <<< "$replica_line"
  local corrupt_addr="${replicas[0]}"
  local corrupt_container
  corrupt_container="$(addr_to_container "$corrupt_addr")"

  local spare_addr
  spare_addr="$(find_spare_addr "${replicas[@]}")"
  local spare_container
  spare_container="$(addr_to_container "$spare_addr")"

  local file_path="/app/chunks/client-1/${dataset_id}/${chunk_id}"
  if ! wait_for_copy "$corrupt_container" "$file_path" 30; then
    echo "1) chunk missing on $corrupt_container"
    return 1
  fi
  echo "1) chunk exists on $corrupt_container"

  local tmpdir
  tmpdir="$(mktemp -d)"
  copy_out "$corrupt_container" "$file_path" "$tmpdir/chunk.bin"
  corrupt_file "$tmpdir/chunk.bin"
  docker cp "$tmpdir/chunk.bin" "${corrupt_container}:${file_path}" >/dev/null
  rm -rf "$tmpdir"
  echo "2) chunk corrupted on $corrupt_container"

  local corrupt_host
  corrupt_host="$(addr_to_host "$corrupt_addr")"
  echo "3) requesting read from $corrupt_host"
  run_read_expect_failure "$corrupt_host" "$chunk_id" "$ARTIFACTS_DIR/corrupt_read.bin"
  echo "4) checksum mismatch reported by storage server"

  if ! wait_for_copy "$spare_container" "$file_path" 60; then
    echo "5) replica did not appear on $spare_container"
    return 1
  fi
  echo "5) replica copied to $spare_container"

  local spare_host
  spare_host="$(addr_to_host "$spare_addr")"
  run_read_expect_success "$spare_host" "$chunk_id" "$ARTIFACTS_DIR/corrupt_read_recovered.bin"
  echo "6) read succeeded from $spare_host"
}

main() {
  run_up
  run_generate

  run_upload
  run_test_server_down
  run_test_corruption

  echo "fault tolerance e2e tests completed"
}

main

