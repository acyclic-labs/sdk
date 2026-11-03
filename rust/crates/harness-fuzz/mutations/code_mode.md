# Interpreter allocation negative control

The `code_mode` target calls the production interpreter with its original native
allocation owner. Its corpus contains captured tool inputs listed in
`corpus/SOURCES.tsv`; the target also checks quoted input against an independent
JSON string value and retires the real tool dispatcher.

From a clean checkout, run the target, apply the mutation, then run the same target:

```sh
cargo +nightly fuzz run code_mode --fuzz-dir rust/crates/harness-fuzz --no-default-features --features code-mode --dev --jobs 1 --target x86_64-unknown-linux-gnu -- -runs=16 -seed=1 -max_len=4096
git apply rust/crates/harness-fuzz/mutations/code_mode_detach_credit.patch
cargo +nightly fuzz run code_mode --fuzz-dir rust/crates/harness-fuzz --no-default-features --features code-mode --dev --jobs 1 --target x86_64-unknown-linux-gnu -- -runs=16 -seed=1 -max_len=4096
```

The mutation detaches the real engine from its ledger. The unchanged oracle must
reject success with zero engine credit at `assert!(result.is_err())`. A compiler
failure, timeout or unrelated crash does not qualify the negative control. Keep
the mutation out of product changes and collect coverage from the unmodified
source only.

The same target now also sends production-encoded success and refusal replies
through the original channel and UTF-8/JSON decoder. Each captured input seeds
the reply's string value; the production encoder produces its framing and
escaping at runtime. The independent oracle checks the returned value or caught
refusal message exactly, including empty strings, Unicode and escaped text.
Arbitrary input bytes also enter the same decoder as malformed frames. Both
encoder and engine use the original native ledger, which must return to its
baseline after retirement. The channel's sole sender belongs to the interpreter;
early engine retirement closes the receiver instead of stranding a dispatcher.

Apply `mutations/code_mode_accept_refusal.patch` to remove the production
interpreter's refusal branch, then run the same command above. The unchanged
oracle must fail with `the original host reply must preserve its value and
refusal`, including on the empty input. This is a decoder mutation, independent
of the allocation-owner mutation. Restore each patch before collecting coverage.

This validates the original reply codec and custody transfer, not host tool
admission, journal settlement or authorization. Finite fuzz runs do not establish
full source branch coverage or C parser coverage.
