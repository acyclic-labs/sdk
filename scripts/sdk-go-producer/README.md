# Go producer tooling

This isolated standard-library tool repairs the staging boundary of the
historical Go producer. It is not an SDK package and is not connected to the
pending foundation generation entrypoint. Do not use historical generated
`go/client.go` as the new public interface.

The imported producer source was read from the parent migration checkout with
SHA-256 `7e3d0ecd856be85d10aaca6e21ea803689d574331f260d07e2d53146feba4feb`.
The existing maintained protoc plugins still generate all transport bindings.
This tool adds no independent service, field, validation or transport semantics.
Importing the tool is source movement, not removal of a custom implementation.

Output must be absent, have an existing parent, and be disjoint from the source,
authority and request paths after resolving aliases. Input hashes and tools are
checked before creating output. The producer never deletes existing output;
failed generation leaves its owned partial directory for inspection. These
checks assume input directories and their parents are not concurrently renamed
or replaced by another actor. They do not constrain arbitrary external tools.
Expected plugin versions are mandatory, resolved tool paths are absolute, and
receipts record the executable hashes. Each authority source must have its
generated `.pb.go` file; unrelated package files cannot satisfy that check.
Descriptors may be shared between families: they are digest-checked separately
and are not passed as protoc source inputs. The receipt hashes the entire
authority manifest, including these descriptor declarations.

Run focused staging tests with `go test -p=1 -parallel=1 ./...` in this directory.
Full acceptance additionally requires the final Rust export, exact plugin/tool
identities, generated-package installation, idiomatic type controls and actual
remote/embedded conformance. Staging tests do not qualify any of those surfaces.
