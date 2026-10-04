#!/usr/bin/env bash
set -euo pipefail

source_root=${1:?Rust source root is required}
output_root=${2:?output root is required}
product_root=${3:-${RUST_PRODUCTS_ROOT:-}}
mkdir -p "$output_root"
proto_root="$source_root/proto"
input_kind=compatibility-projection
if [[ -n "$product_root" && -d "$product_root" ]]; then
  proto=$(find "$product_root" -type f -path '*/actors/v1/actors.proto' -print -quit)
  [[ -n "$proto" ]] || proto=$(find "$product_root" -type f -name actors.proto -print -quit)
  [[ -n "$proto" ]] || { echo 'Rust product output has no generated Actors protobuf' >&2; exit 1; }
  input_kind=generated-rust-product
else
  proto="$proto_root/actors/v1/actors.proto"
  [[ -f "$proto" ]] || { echo 'Rust-owned Actors protobuf is missing' >&2; exit 1; }
fi

lua_repo=https://github.com/Protocol-Lattice/lua-grpc.git
lua_revision=1247ad7278ec39b3e2db384ede8ca6d1e6204fa1
lua_checkout="$output_root/lua-grpc"
git clone --filter=blob:none --no-checkout "$lua_repo" "$lua_checkout"
git -C "$lua_checkout" checkout --detach "$lua_revision"
license_status=unverified-repository-license
if find "$lua_checkout" -maxdepth 1 -type f \( -iname 'license*' -o -iname 'copying*' \) -print -quit | grep -q .; then license_status=repository-license-present; fi

pushd "$lua_checkout" >/dev/null
go test ./...
go vet ./...
make build
luarocks make --deps-mode=all lua-grpc-0.1.6-1.rockspec
rock=$(luarocks pack lua-grpc 0.1.6-1 | tail -n 1)
[[ -s "$rock" ]]
cp "$rock" "$output_root/"
popd >/dev/null

generated="$output_root/generated"
mkdir -p "$generated"
protoc -I "$proto_root" -I "$output_root" --descriptor_set_out="$generated/actors.pb" --include_imports --plugin="$lua_checkout/bin/protoc-gen-lua-grpc" --lua-grpc_out="$generated" --lua-grpc_opt=paths=source_relative "$proto"
binding=$(find "$generated" -type f -name '*_grpc.lua' -print -quit)
[[ -n "$binding" ]] || { echo 'Lua generator produced no service binding' >&2; exit 1; }
cp "$binding" "$generated/actors_grpc.lua"

cat >"$generated/consumer.lua" <<'LUA'
local grpc = require("grpc")
assert(grpc.load_descriptor_set("actors.pb"))
local generated = assert(require("actors_grpc"))
local service
for _, candidate in pairs(generated) do
  if type(candidate) == "table" and type(candidate.methods) == "table" then service = candidate break end
end
assert(service, "generated Rust-owned service metadata missing")
local seen = { unary = false, server_streaming = false, client_streaming = false, bidi = false }
for _, method in pairs(service.methods) do
  assert(type(method.path) == "string" and method.path:match("^/"))
  assert(type(method.input_type) == "string" and type(method.output_type) == "string")
  if method.client_streaming and method.server_streaming then seen.bidi = true
  elseif method.client_streaming then seen.client_streaming = true
  elseif method.server_streaming then seen.server_streaming = true
  else seen.unary = true end
end
for shape, value in pairs(seen) do assert(value, "missing generated RPC shape: " .. shape) end
local context = require("grpc.context")
local ctx = context.new({ timeout = 0.05 })
assert(ctx:remaining_timeout() >= 0)
ctx:cancel("qualification")
assert(ctx:is_cancelled() and ctx.cancel_reason == "qualification")
print("Rust-owned Lua generated consumer passed")
LUA
(cd "$generated" && lua consumer.lua)

pushd "$lua_checkout" >/dev/null
lua tests/lua/run.lua
popd >/dev/null
proto_sha256=$(sha256sum "$proto" | awk '{print $1}')
descriptor_sha256=$(sha256sum "$generated/actors.pb" | awk '{print $1}')
cat >"$output_root/qualification.json" <<EOF
{"schema":"acyclic.additional-language-qualification.v1","language":"lua","status":"passed","source_revision":"${GITHUB_SHA:-local}","input_kind":"$input_kind","proto_sha256":"$proto_sha256","descriptor_sha256":"$descriptor_sha256","generator":{"repository":"$lua_repo","revision":"$lua_revision","license_status":"$license_status"},"evidence":["go test ./...","go vet ./...","generated Rust-owned descriptor and four-shape consumer","upstream Lua runtime suite including HTTP/2 transport, deadlines and cancellation"],"artifact_root":"$output_root"}
EOF
