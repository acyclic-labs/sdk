# Python and Go release qualification

`release-python-consumer.py` and `release-go-consumer.go` derive their method
sets from `rust-authority.json`. They call all 106 Rust-authority RPCs against
the Rust fixture and capture deterministic protobuf bytes for every request
frame and response frame. Client-streaming calls capture every sent frame;
server-streaming calls preserve response order; empty streams retain their
terminal gRPC status.

`validate-runtime-frames.py` is deliberately stricter than a reachability
probe. It requires an installed consumer to report a passed remote call,
source identity, contiguous raw frames with matching SHA-256 values, and an
exact one-to-one match with the 106-method Rust inventory. The shared Rust
semantic verifier remains the semantic gate; language adapters normalize their
receipts into its canonical observation shape.

The workflow at `.github/workflows/python-go-release-qualification.yml` runs
only for a published release, manual dispatch, or workflow call. It pins
Python, grpcio/protobuf, Go, protoc, and the Go protobuf plugins; builds and
installs the Python wheel; generates the Go package through the Rust-bound
producer; compiles both consumers; starts the Rust fixture; and uploads the
receipts. Pull requests keep the fast Rust policy lane and do not install
downstream runtimes.
