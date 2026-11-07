`native_hooks` checks the production host-version decoder and installed-hook
ownership decoder. Every input must admit the original supported host output,
refuse the original obsolete host output, and recognize all four groups from
the public `claude_hooks` writer before arbitrary-input checks run.

The two stdout seeds are byte-for-byte copies of the official native binaries'
captures in `rust/crates/harness/fixtures/hosts`. Their original command,
artifact integrity, timestamps and output hashes are in that directory's
`provenance.json`. They are not synthetic version strings.

Capture the hook-group seeds through the actual SDK writer:

```sh
cargo run --locked -p acyclic-sdk-fuzz --example capture_native_hooks -- rust/fuzz/corpus/native_hooks
node rust/proofs/qualify.mjs native-hook-admission
```

The qualifier independently raises the production admission floor, lowers it
to admit the obsolete captured host, and removes ownership of writer output.
Each retained mutation must hit its named assertion before exact source
restoration. One restored 1,024-input instrumented campaign follows all three
negatives. The original version and ownership examples are not duplicated.

This is pure codec validation, with no host process, file watcher or callback
wait. Work is bounded by input bytes plus two fixed captured versions and four
fixed writer groups; it establishes no asynchronous progress guarantee.

The four retained hook-group files were captured on Linux x86_64 from the
command above at source `fedb595178c29feb1a8b1f04783fb00968d7d518`. The files preserve the exact
JSON bytes printed by the public SDK writer, without reserialization.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `PermissionRequest` | 212 | `62227fad9b73ae6e3536cdd69ac6b0269a367353e52531785b55f8b2f24e154e` |
| `SessionEnd` | 145 | `4cf208aff56b16114e023d9801ed62fce6a6001b054fcb28a7f1bae435056a45` |
| `SessionStart` | 149 | `b9f395613d7e075fde74e49b48fe63f4d4b281d41d96318b74e1505b61fbadad` |
| `Stop` | 141 | `b32df880dd3b8b6692edc851b96d6e0734ff5626e269a332c431485f0d6695b0` |
