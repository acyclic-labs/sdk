#!/usr/bin/env bash
set -euo pipefail

source_root=${1:?Rust source root is required}
output_root=${2:?output root is required}
mkdir -p "$output_root"

manifest="$source_root/rust/crates/sdk-embedded-prototype/Cargo.toml"
[[ -f "$manifest" ]] || { echo 'Rust embedded ABI manifest is missing' >&2; exit 1; }
cargo build --locked --manifest-path "$manifest" --release

case "$(uname -s)" in
  Linux*) library="$source_root/rust/crates/sdk-embedded-prototype/target/release/libacyclic_sdk_embedded_prototype.so" ;;
  Darwin*) library="$source_root/rust/crates/sdk-embedded-prototype/target/release/libacyclic_sdk_embedded_prototype.dylib" ;;
  MINGW*|MSYS*|CYGWIN*) library="$source_root/rust/crates/sdk-embedded-prototype/target/release/acyclic_sdk_embedded_prototype.dll" ;;
  *) echo "unsupported LuaJIT host: $(uname -s)" >&2; exit 2 ;;
esac
[[ -s "$library" ]] || { echo "Rust ABI library was not produced: $library" >&2; exit 1; }

cp "$library" "$output_root/"
library_name=$(basename "$library")
cat >"$output_root/consumer.lua" <<'LUA'
local ffi = require("ffi")
ffi.cdef[[
typedef struct { uint64_t id; uint8_t *ptr; size_t len; size_t capacity; } AcyclicBuffer;
typedef struct { uint32_t status; uint64_t start; uint64_t end; uint64_t tail; AcyclicBuffer message; } AcyclicAppendResult;
typedef struct { uint32_t status; uint64_t reader; AcyclicBuffer message; } AcyclicOpenResult;
typedef struct { uint32_t status; uint64_t sequence; AcyclicBuffer value; AcyclicBuffer message; } AcyclicNextResult;
uint32_t acyclic_embedded_abi_version(void);
uint64_t acyclic_embedded_engine_open(void);
void acyclic_embedded_engine_close(uint64_t engine);
AcyclicAppendResult acyclic_embedded_engine_append(uint64_t engine, const uint8_t *path, size_t path_len, const uint8_t *value, size_t value_len);
AcyclicOpenResult acyclic_embedded_reader_open(uint64_t engine, const uint8_t *path, size_t path_len, uint64_t from, uint32_t limit, uint32_t mode);
AcyclicNextResult acyclic_embedded_reader_next(uint64_t reader);
void acyclic_embedded_reader_cancel(uint64_t reader);
void acyclic_embedded_reader_close(uint64_t reader);
uint32_t acyclic_buffer_release(AcyclicBuffer buffer);
void acyclic_append_result_release(AcyclicAppendResult result);
void acyclic_open_result_release(AcyclicOpenResult result);
void acyclic_next_result_release(AcyclicNextResult result);
]]

local sdk = assert(ffi.load(arg[1]))
assert(sdk.acyclic_embedded_abi_version() == 1)
local engine = assert(sdk.acyclic_embedded_engine_open())
local path = "lua-ffi"
local payload = "rust-owned"
local appended = sdk.acyclic_embedded_engine_append(engine, path, #path, payload, #payload)
assert(appended.status == 0, "append failed")
assert(appended.end == 1)
sdk.acyclic_append_result_release(appended)

local opened = sdk.acyclic_embedded_reader_open(engine, path, #path, 0, 8, 0)
assert(opened.status == 0 and opened.reader ~= 0, "read open failed")
local next_value = sdk.acyclic_embedded_reader_next(opened.reader)
assert(next_value.status == 0 and ffi.string(next_value.value.ptr, next_value.value.len) == payload)
sdk.acyclic_next_result_release(next_value)
local ended = sdk.acyclic_embedded_reader_next(opened.reader)
assert(ended.status == 1, "finite reader did not end")
sdk.acyclic_next_result_release(ended)
sdk.acyclic_embedded_reader_close(opened.reader)

local follow = sdk.acyclic_embedded_reader_open(engine, path, #path, 1, 8, 1)
assert(follow.status == 0 and follow.reader ~= 0, "follow open failed")
sdk.acyclic_embedded_reader_cancel(follow.reader)
local cancelled = sdk.acyclic_embedded_reader_next(follow.reader)
assert(cancelled.status == 3, "follow cancellation did not cross the Rust ABI")
sdk.acyclic_next_result_release(cancelled)
sdk.acyclic_embedded_reader_close(follow.reader)
sdk.acyclic_embedded_engine_close(engine)
print("LuaJIT FFI Rust embedded ABI consumer passed")
LUA

luajit "$output_root/consumer.lua" "$output_root/$library_name"
sha256=$(sha256sum "$output_root/$library_name" | awk '{print $1}')
cat >"$output_root/qualification.json" <<EOF
{"schema":"acyclic.lua.ffi-qualification.v1","language":"lua","status":"passed","scope":"embedded-rust-c-abi","source_crate":"rust/crates/sdk-embedded-prototype","abi_version":1,"library":"$library_name","library_sha256":"$sha256","consumer":"LuaJIT FFI append/read/follow-cancel","license":"Apache-2.0 from Rust workspace crate"}
EOF
