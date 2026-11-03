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

The same target covers accepted tool and console RPCs whose original reply
producer is dropped, and a console RPC settled with the original `None` reply.
These cases cross the production blocking-pool boundary and retain the original
native ledger. Each request must actually be received before producer retirement;
the interpreter must refuse, release its credit and finish without another wake.
The successful console settlement must return null. This subsumes the former
standalone dispatcher-drop example while adding native custody and variable input.

Apply `mutations/code_mode_retired_reply_succeeds.patch` to turn the production
oneshot cancellation into an empty successful reply. The unchanged oracle must
fail at `a retired reply producer must refuse` on the console case. This mutation
does not strand a future: it exercises the wrong-success failure mode. Restore
the patch before collecting original coverage.

The same receive boundary also admits cancellation for tool and console calls.
The dispatcher cancels the original token while retaining its original reply
sender until the interpreter finishes. Reply-sender loss therefore cannot cause
that completion. The exact interruption and native-credit retirement are checked
through the original blocking pool, with no timer or readiness loop. The separate
StockExecution journal-cancellation test retains its distinct terminal-record check.

Apply `mutations/code_mode_cancel_succeeds.patch` to make the production cancellation
selection return null instead of its interruption. The unchanged oracle must fail
at `parent cancellation must retain its interruption`. This wrong-success mutation
keeps the original cancellation wake and introduces no never-settled future.
