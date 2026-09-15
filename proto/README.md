# Public protocol source

Protobuf is the canonical cross-language contract. Each schema uses an explicit
versioned package: Objects, Machines, Inference, and Harness currently use v1;
Stream and Filesystem use v2. HTTP/JSON annotations and generated OpenAPI are
added with the first audited service RPCs. Generated transport types are not the
language SDK's public API.
