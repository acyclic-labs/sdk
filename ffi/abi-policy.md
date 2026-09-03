# ABI policy

The future ABI uses opaque handles, caller-independent owned byte buffers,
explicit release functions, and callback or poll-based asynchronous completion.
Rust layouts, references, pointers, futures, trait objects, and panics never cross
the boundary. Every exported symbol belongs to a versioned ABI surface and must
run the language-neutral conformance vectors before release.
