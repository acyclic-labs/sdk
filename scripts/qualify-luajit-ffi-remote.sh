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
typedef struct { uint32_t status; uint64_t reader; uint64_t writer; AcyclicRemoteBuffer message; } AcyclicRemoteDuplexOpenResult;
typedef struct { uint32_t status; uint64_t sequence; AcyclicRemoteBuffer value; AcyclicRemoteBuffer message; } AcyclicRemoteNextResult;
typedef struct { uint32_t status; AcyclicRemoteBuffer response; AcyclicRemoteBuffer message; } AcyclicRemoteWireResult;
uint32_t acyclic_remote_abi_version(void);
size_t acyclic_remote_family_count(void);
AcyclicRemoteBuffer acyclic_remote_family_name(size_t index);
size_t acyclic_remote_family_operation_count(const uint8_t *family, size_t family_len);
AcyclicRemoteBuffer acyclic_remote_family_operation_name(const uint8_t *family, size_t family_len, size_t index);
AcyclicRemoteWireResult acyclic_remote_family_wire_call(uint64_t client, const uint8_t *family, size_t family_len, const uint8_t *operation, size_t operation_len, const uint8_t *request, size_t request_len);
AcyclicRemoteOpenResult acyclic_remote_family_stream_open(uint64_t client, const uint8_t *family, size_t family_len, const uint8_t *operation, size_t operation_len, const uint8_t *request, size_t request_len);
AcyclicRemoteDuplexOpenResult acyclic_remote_family_duplex_open(uint64_t client, const uint8_t *family, size_t family_len, const uint8_t *operation, size_t operation_len);
uint32_t acyclic_remote_stream_write(uint64_t writer, const uint8_t *request, size_t request_len);
uint32_t acyclic_remote_stream_finish(uint64_t writer);
uint32_t acyclic_remote_stream_cancel(uint64_t writer);
void acyclic_remote_stream_close(uint64_t writer);
size_t acyclic_remote_stream_operation_count(void);
AcyclicRemoteBuffer acyclic_remote_stream_operation_name(size_t index);
AcyclicRemoteWireResult acyclic_remote_wire_call(uint64_t client, const uint8_t *operation, size_t operation_len, const uint8_t *request, size_t request_len);
void acyclic_remote_wire_result_release(AcyclicRemoteWireResult result);
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
void acyclic_remote_duplex_open_result_release(AcyclicRemoteDuplexOpenResult result);
void acyclic_remote_next_result_release(AcyclicRemoteNextResult result);
]]
local sdk = assert(ffi.load(arg[1]))
assert(sdk.acyclic_remote_abi_version() == 1)
local family_count = sdk.acyclic_remote_family_count()
assert(family_count == 8, "Rust contract family inventory is incomplete")
local family_operation_total = 0
for i = 0, family_count - 1 do
  local family_item = sdk.acyclic_remote_family_name(i)
  assert(family_item.len > 0, "Rust family inventory contains an empty name")
  local family = ffi.string(family_item.ptr, family_item.len)
  sdk.acyclic_remote_buffer_release(family_item)
  local operation_count = sdk.acyclic_remote_family_operation_count(family, #family)
  assert(operation_count > 0, "Rust family has no descriptor operation inventory: " .. family)
  family_operation_total = family_operation_total + operation_count
  for operation_index = 0, operation_count - 1 do
    local operation_item = sdk.acyclic_remote_family_operation_name(family, #family, operation_index)
    assert(operation_item.len > 0, "Rust operation inventory contains an empty name")
    sdk.acyclic_remote_buffer_release(operation_item)
  end
end
assert(family_operation_total == 106, "Rust product operation inventory changed")
local expected = {"inspect_idempotency", "append", "tail", "fork", "read", "follow", "children", "children_page", "commit", "read_commit"}
assert(sdk.acyclic_remote_stream_operation_count() == #expected, "Rust operation inventory is incomplete")
for i, name in ipairs(expected) do
  local item = sdk.acyclic_remote_stream_operation_name(i - 1)
  assert(item.len == #name and ffi.string(item.ptr, item.len) == name, "Rust operation inventory changed")
  sdk.acyclic_remote_buffer_release(item)
end
local endpoint = assert(arg[2])
local ca_file = assert(io.open(arg[3], "rb"))
local ca = ca_file:read("*a")
ca_file:close()
local token = "lua-fixture-token"
local client = sdk.acyclic_remote_client_open(endpoint, #endpoint, token, #token, ca, #ca)
assert(client ~= 0, "Rust remote client failed to connect")
local unsupported_family = sdk.acyclic_remote_family_wire_call(client, "machines", 8, "unsupported", 11, nil, 0)
assert(unsupported_family.status == 4, "Rust ABI did not fail closed for a family without an HTTP projection")
sdk.acyclic_remote_wire_result_release(unsupported_family)
local upload = sdk.acyclic_remote_family_duplex_open(client, "objects", 7, "putObject", 9)
assert(upload.status == 0 and upload.reader ~= 0 and upload.writer ~= 0, "Rust client-streaming upload did not open")
local header = string.char(10, 12, 18, 10) .. "lua-upload"
local body = string.char(18, 8) .. "lua-body"
local complete = string.char(24, 1)
assert(sdk.acyclic_remote_stream_write(upload.writer, header, #header) == 0, "client-streaming header write failed")
assert(sdk.acyclic_remote_stream_write(upload.writer, body, #body) == 0, "client-streaming body write failed")
assert(sdk.acyclic_remote_stream_write(upload.writer, complete, #complete) == 0, "client-streaming completion write failed")
assert(sdk.acyclic_remote_stream_finish(upload.writer) == 0, "client-streaming finish failed")
local upload_result = sdk.acyclic_remote_reader_next(upload.reader)
assert(upload_result.status == 0 and upload_result.value.len > 0, "client-streaming response was not received")
sdk.acyclic_remote_next_result_release(upload_result)
local upload_end = sdk.acyclic_remote_reader_next(upload.reader)
assert(upload_end.status == 1, "client-streaming response did not end")
sdk.acyclic_remote_next_result_release(upload_end)
sdk.acyclic_remote_reader_close(upload.reader)
sdk.acyclic_remote_stream_close(upload.writer)
local cancelled_upload = sdk.acyclic_remote_family_duplex_open(client, "objects", 7, "putObject", 9)
assert(cancelled_upload.status == 0, "cancel lifecycle upload did not open")
sdk.acyclic_remote_reader_cancel(cancelled_upload.reader)
assert(sdk.acyclic_remote_stream_cancel(cancelled_upload.writer) == 3, "client-streaming cancellation did not cross the writer ABI")
local cancelled_upload_next = sdk.acyclic_remote_reader_next(cancelled_upload.reader)
assert(cancelled_upload_next.status == 3, "client-streaming reader cancellation did not cross the Rust ABI")
sdk.acyclic_remote_next_result_release(cancelled_upload_next)
sdk.acyclic_remote_reader_close(cancelled_upload.reader)
sdk.acyclic_remote_stream_close(cancelled_upload.writer)
local path, payload = "lua-remote-ffi", "rust-owned-remote"
local appended = sdk.acyclic_remote_append(client, path, #path, payload, #payload)
assert(appended.status == 0 and appended.end == 1, "remote append failed")
sdk.acyclic_remote_append_result_release(appended)
local tail_request = string.char(10, #path) .. path
local wire = sdk.acyclic_remote_wire_call(client, "tail", 4, tail_request, #tail_request)
assert(wire.status == 0, "Rust generic wire dispatch failed")
sdk.acyclic_remote_wire_result_release(wire)
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
hash_file() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}
source_revision=$(git -C "$source_root" rev-parse HEAD 2>/dev/null || printf 'local')
lock_sha256=$(hash_file "$source_root/Cargo.lock")
fixture_ca_sha256=$(hash_file "$ca")
descriptor_sha256=$(find "$source_root/rust/crates/sdk-contract-wire/src" -type f -name '*.rs' -print \
  | LC_ALL=C sort \
  | while IFS= read -r file; do
      printf '%s  %s\n' "$(hash_file "$file")" "${file#"$source_root/"}"
    done \
  | if command -v sha256sum >/dev/null; then sha256sum | awk '{print $1}'; else shasum -a 256 | awk '{print $1}'; fi)
cat >"$output_root/LICENSE-RUST-APACHE-2.0.txt" <<'EOF'
Rust remote C ABI crate license: Apache-2.0. See https://www.apache.org/licenses/LICENSE-2.0
EOF
cat >"$output_root/LICENSE-LUAJIT-MIT.txt" <<'EOF'
LuaJIT runtime license: MIT. See https://github.com/LuaJIT/LuaJIT/blob/v2.1/COPYRIGHT
EOF
cat >"$output_root/source-provenance.json" <<EOF
{"schema":"acyclic.lua.ffi-source-provenance.v1","source_revision":"$source_revision","cargo_lock_sha256":"$lock_sha256","rust_contract_source_sha256":"$descriptor_sha256","fixture_ca_sha256":"$fixture_ca_sha256","contract_source_root":"rust/crates/sdk-contract-wire/src","fixture":"rust/crates/sdk-luajit-remote/src/bin/remote-fixture.rs","abi":"rust/crates/sdk-luajit-remote/src/lib.rs"}
EOF
tar -czf "$output_root/acyclic_sdk_luajit_remote.tar.gz" -C "$output_root" "$library_name" consumer.lua LICENSE-RUST-APACHE-2.0.txt LICENSE-LUAJIT-MIT.txt source-provenance.json
cat >"$output_root/qualification.json" <<EOF
{"schema":"acyclic.lua.ffi-qualification.v1","language":"lua","status":"passed","scope":"remote-rust-c-abi","source_revision":"$source_revision","source_crate":"rust/crates/sdk-luajit-remote","abi_version":1,"library":"$library_name","library_sha256":"$sha256","consumer":"LuaJIT FFI Rust remote generated Stream operations plus append/read/follow-cancel and descriptor-driven client-streaming handles","rust_family_inventory":8,"rust_product_operation_inventory":106,"rust_operation_inventory":10,"rust_contract_source_sha256":"$descriptor_sha256","fixture_ca_sha256":"$fixture_ca_sha256","rust_json_dispatch":"Rust-owned HTTP route projections","rust_stream_input_queue_capacity":64,"rust_transport":"Rust descriptor-driven gRPC with bounded FFI request backpressure","rust_crate_license":"Apache-2.0","luajit_runtime":"LuaJIT 2.1","luajit_runtime_license":"MIT","archive":"acyclic_sdk_luajit_remote.tar.gz","provenance":"source-provenance.json"}
EOF
