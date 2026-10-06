# Lua OpenAPI target prototype

This directory records a bounded Lua HTTP/JSON experiment. The input is a
Rust-owned OpenAPI projection; no Lua contract, route table, validation rule,
or transport policy is authored here.

The pinned generator is OpenAPI Generator 7.25.0, released under Apache-2.0,
with the SHA-256 recorded in `manifest.json`. Its official Lua client is
labelled **BETA**. The generator produced a LuaRocks rockspec and a client
using `lua-http`, `dkjson`, and `basexx` from the Workers projection. The
prototype used an identifier-safe package name because the Lua template emits
package names into local identifiers.

The Rust projection preserves unsigned 64-bit values as decimal JSON strings:
the Workers document contains eight `x-protobuf-json: decimal-string`
annotations and the full unsigned range. The generated Lua models do not
mention those Rust-owned extensions and do not validate the decimal pattern,
presence, or oneof metadata. Callers can pass a decimal string through the
generic table models, but the generated client does not provide a proof of
that contract. This is therefore not lossless SDK qualification.

The generated package is also marked `Unlicense` by the upstream template and
contains placeholder repository metadata. It needs a Rust-owned package
metadata adapter before it could be an acyclic.dev artifact. No Lua or
LuaRocks runtime is installed in the qualification environment, so the
rockspec was not installed and the consumer was not executed.

## Decision

Keep Lua excluded from the full SDK target set. It is a useful HTTP/JSON
prototype and a candidate for a future explicitly bounded remote subset after
Rust-owned metadata and validation adapters exist. It does not qualify the
native gRPC, streaming, recovery, cancellation, or embedded surfaces.

The surrounding OSS options do not close that gap. `starwing/lua-protobuf`
provides protobuf encoding/decoding for Lua 5.1--5.4 and LuaJIT, but it is not
a gRPC client/runtime. `lua-http` provides HTTP/1, HTTP/2, WebSocket, and TLS
building blocks; `lua-resty-http` is an OpenResty HTTP client. Neither supplies
the generated RPC, descriptor handshake, or Rust transport policy. The
prototype deliberately does not claim those capabilities.

Primary references:

- [OpenAPI Generator Lua client](https://openapi-generator.tech/docs/generators/lua/)
- [OpenAPI Generator 7.25.0 license](https://github.com/OpenAPITools/openapi-generator/blob/v7.25.0/LICENSE)
- [lua-protobuf](https://github.com/starwing/lua-protobuf)
- [lua-http](https://github.com/daurnimator/lua-http/blob/master/doc/introduction.md)
- [lua-resty-http](https://github.com/ledgetech/lua-resty-http)

Run `prototype.ps1` with a Rust-generated OpenAPI document to reproduce the
generation and emit a source-bound receipt. The generated tree and receipt
belong in an external staging directory, not in this source directory.
