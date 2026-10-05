# Public protocol source

Protobuf is the canonical cross-language contract. Each schema uses an explicit
versioned package: Objects, Machines, Inference, Actors, and Workers use v1;
Stream, Filesystem, and Harness use v2. `protocol/v1` holds the version handshake every service
family shares, so no family imports another family's schema. HTTP/JSON
annotations and generated OpenAPI are added with the first audited service
RPCs. Existing families expose curated language SDK types. Actors and Workers
also export their generated v1 request and response types as public contracts.
