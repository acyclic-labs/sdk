#!/usr/bin/env bash
set -euo pipefail
source_root=${1:?Rust source root is required}
output_root=${2:?output root is required}
mkdir -p "$output_root"
manifest="$source_root/rust/crates/sdk-luajit-remote/Cargo.toml"
[[ -f "$manifest" ]] || { echo 'Rust LuaJIT remote ABI manifest is missing' >&2; exit 1; }
target_dir="$output_root/rust-target"
cargo build --locked --manifest-path "$manifest" --release --target-dir "$target_dir"
case "$(uname -s)" in
  Linux*) library="$target_dir/release/libacyclic_sdk_luajit_remote.so" ;;
  Darwin*) library="$target_dir/release/libacyclic_sdk_luajit_remote.dylib" ;;
  MINGW*|MSYS*|CYGWIN*) library="$target_dir/release/acyclic_sdk_luajit_remote.dll" ;;
  *) echo "unsupported LuaJIT host: $(uname -s)" >&2; exit 2 ;;
esac
[[ -s "$library" ]] || { echo "Rust ABI library was not produced: $library" >&2; exit 1; }
cp "$library" "$output_root/"
library_name=$(basename "$library")
fixture="$target_dir/release/luajit-remote-fixture"
ca="$output_root/fixture-ca.pem"
"$fixture" --ca-path "$ca" >"$output_root/fixture-endpoint.txt" 2>"$output_root/fixture.log" &
fixture_pid=$!
cleanup() { kill "$fixture_pid" 2>/dev/null || true; wait "$fixture_pid" 2>/dev/null || true; }
trap cleanup EXIT
for _ in $(seq 1 100); do
  [[ -s "$ca" && -s "$output_root/fixture-endpoint.txt" ]] && break
  sleep 0.1
done
[[ -s "$ca" && -s "$output_root/fixture-endpoint.txt" ]] || { cat "$output_root/fixture.log" >&2; exit 1; }
endpoint=$(head -n 1 "$output_root/fixture-endpoint.txt")
cat >"$output_root/consumer.lua" <<'LUA'
local ffi = require("ffi")
ffi.cdef[[
typedef struct { uint64_t id; uint8_t *ptr; size_t len; size_t capacity; } AcyclicRemoteBuffer;
typedef struct { uint32_t status; uint64_t start; uint64_t end; uint64_t tail; AcyclicRemoteBuffer message; } AcyclicRemoteAppendResult;
typedef struct { uint32_t status; uint64_t reader; AcyclicRemoteBuffer message; } AcyclicRemoteOpenResult;
typedef struct { uint32_t status; uint64_t sequence; AcyclicRemoteBuffer value; AcyclicRemoteBuffer message; } AcyclicRemoteNextResult;
uint32_t acyclic_remote_abi_version(void);
uint64_t acyclic_remote_client_open(const uint8_t *endpoints, size_t endpoints_len, const uint8_t *token, size_t token_len, const uint8_t *ca, size_t ca_len);
void acyclic_remote_client_close(uint64_t client);
AcyclicRemoteAppendResult acyclic_remote_append(uint64_t client, const uint8_t *path, size_t path_len, const uint8_t *value, size_t value_len);
AcyclicRemoteOpenResult acyclic_remote_reader_open(uint64_t client, const uint8_t *path, size_t path_len, uint64_t from, uint32_t limit, uint32_t mode);
AcyclicRemoteNextResult acyclic_remote_reader_next(uint64_t reader);
void acyclic_remote_reader_cancel(uint64_t reader);
void acyclic_remote_reader_close(uint64_t reader);
uint32_t acyclic_remote_buffer_release(AcyclicRemoteBuffer buffer);
void acyclic_remote_append_result_release(AcyclicRemoteAppendResult result);
void acyclic_remote_open_result_release(AcyclicRemoteOpenResult result);
void acyclic_remote_next_result_release(AcyclicRemoteNextResult result);
]]
local sdk = assert(ffi.load(arg[1]))
assert(sdk.acyclic_remote_abi_version() == 1)
local endpoint = assert(arg[2])
local ca_file = assert(io.open(arg[3], "rb"))
local ca = ca_file:read("*a")
ca_file:close()
local token = "lua-fixture-token"
local client = sdk.acyclic_remote_client_open(endpoint, #endpoint, token, #token, ca, #ca)
assert(client ~= 0, "Rust remote client failed to connect")
local path, payload = "lua-remote-ffi", "rust-owned-remote"
local appended = sdk.acyclic_remote_append(client, path, #path, payload, #payload)
assert(appended.status == 0 and appended.end == 1, "remote append failed")
sdk.acyclic_remote_append_result_release(appended)
local opened = sdk.acyclic_remote_reader_open(client, path, #path, 0, 8, 0)
assert(opened.status == 0 and opened.reader ~= 0, "remote read open failed")
local next_value = sdk.acyclic_remote_reader_next(opened.reader)
assert(next_value.status == 0 and ffi.string(next_value.value.ptr, next_value.value.len) == payload, "remote read changed the payload")
sdk.acyclic_remote_next_result_release(next_value)
local ended = sdk.acyclic_remote_reader_next(opened.reader)
assert(ended.status == 1, "remote finite reader did not end")
sdk.acyclic_remote_next_result_release(ended)
sdk.acyclic_remote_reader_close(opened.reader)
local follow = sdk.acyclic_remote_reader_open(client, path, #path, 1, 0, 1)
assert(follow.status == 0 and follow.reader ~= 0, "remote follow open failed")
sdk.acyclic_remote_reader_cancel(follow.reader)
local cancelled = sdk.acyclic_remote_reader_next(follow.reader)
assert(cancelled.status == 3, "remote follow cancellation did not cross the Rust ABI")
sdk.acyclic_remote_next_result_release(cancelled)
sdk.acyclic_remote_reader_close(follow.reader)
sdk.acyclic_remote_client_close(client)
print("LuaJIT FFI Rust remote append/read/follow-cancel consumer passed")
LUA
luajit "$output_root/consumer.lua" "$output_root/$library_name" "$endpoint" "$ca"
if command -v sha256sum >/dev/null; then
  sha256=$(sha256sum "$output_root/$library_name" | awk '{print $1}')
else
  sha256=$(shasum -a 256 "$output_root/$library_name" | awk '{print $1}')
fi
cat >"$output_root/LICENSE-RUST-APACHE-2.0.txt" <<'EOF'
Rust remote C ABI crate license: Apache-2.0. See https://www.apache.org/licenses/LICENSE-2.0
EOF
cat >"$output_root/LICENSE-LUAJIT-MIT.txt" <<'EOF'
LuaJIT runtime license: MIT. See https://github.com/LuaJIT/LuaJIT/blob/v2.1/COPYRIGHT
EOF
tar -czf "$output_root/acyclic_sdk_luajit_remote.tar.gz" -C "$output_root" "$library_name" consumer.lua LICENSE-RUST-APACHE-2.0.txt LICENSE-LUAJIT-MIT.txt
cat >"$output_root/qualification.json" <<EOF
{"schema":"acyclic.lua.ffi-qualification.v1","language":"lua","status":"passed","scope":"remote-rust-c-abi","source_crate":"rust/crates/sdk-luajit-remote","abi_version":1,"library":"$library_name","library_sha256":"$sha256","consumer":"LuaJIT FFI Rust remote append/read/follow-cancel","rust_crate_license":"Apache-2.0","luajit_runtime":"LuaJIT 2.1","luajit_runtime_license":"MIT","archive":"acyclic_sdk_luajit_remote.tar.gz"}
EOF