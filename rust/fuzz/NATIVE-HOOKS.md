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
