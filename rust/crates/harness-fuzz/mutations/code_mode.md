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
through the original channel and UTF-8/JSON decoder. Input text seeds the refusal string; the production encoder produces framing
and escaping at runtime. The oracle checks the caught refusal message exactly,
including empty strings, Unicode and escaped text. Successful JSON values use the
same-engine differential described below.
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

Quoted inputs also run at their exact JSON UTF-8 byte allowance and one byte
below it, retaining the native ledger through original encoding and retirement.
The independent Rust JSON string encoding supplies the expected byte length and
value; both the accepted and refused boundary must match exactly. This replaces
the redundant second denied-construction run, rather than increasing engine runs
for valid UTF-8 input. JSON string encoding always includes its two quotes, so the
one-byte decrement cannot underflow.

Apply `mutations/code_mode_output_extra_byte.patch` to let the original interpreter
accept one extra byte. The unchanged oracle must fail at `quoted output must
refuse one byte beyond its admission`, including on empty input. Restore the
mutation before collecting original coverage.

The same target also checks the runtime's original producerless-promise refusal
through the native blocking pool with its original allocation ledger. The real
dispatcher receiver remains alive but supplies no reply or cancellation. Only
the runtime's idle event can finish execution, and retirement must release its
native credit. This subsumes the former unfunded, directly polled example; the
separate StockExecution producerless control keeps its tool and journal checks.
The single-poll check runs inside that blocking boundary and rejects a pending
future immediately, preserving the progress oracle without a timeout.

Apply `mutations/code_mode_producerless_succeeds.patch` to return null from the
original producerless refusal branch. The unchanged oracle must fail at
`a producerless promise must retain its refusal`. This wrong-success mutation
preserves the idle event and introduces no stranded future. Restore it before
collecting original coverage.

The same progress relation now checks finite CPU work against the original
protective interrupt counter, retaining the native ledger and one-poll blocking
boundary. The finite loop completes even if interruption is broken, so neither
the positive nor negative needs a timeout. This subsumes the former unfunded
infinite-loop example. The target's private checks separate program/output,
progress, reply and settlement obligations without another model or executor.

Apply `mutations/code_mode_ignore_interrupt_budget.patch` to prevent the original
remaining-budget condition from firing. The unchanged oracle must fail at
`finite CPU work must reach its protective interrupt` when the loop finishes
without interruption. Restore the mutation before collecting original coverage.

Successful host replies also carry arbitrary JSON values. Valid JSON inputs seed
the original reply encoder directly; other inputs generate a composite value with
every JSON kind, nested containers, extreme integers, negative zero and the input
text. These are generated properties, not captured host-reply fixtures. Refusals
retain the production producer's string shape. The differential oracle evaluates
the serializer's JSON through the same production engine without a tool reply,
then compares its completion to the value delivered through the original reply
channel. This retains JavaScript number and Unicode semantics without another
decoder or a Rust model of coercion. Both executions retire their native credit.

Apply `mutations/code_mode_nonstring_reply_null.patch` to preserve strings but
replace other successful reply values with null. The earlier string-only oracle
passes this mutation; the value-complete differential must reject it at
`the original host reply must preserve its value and refusal`. Restore the patch
before collecting original coverage. Arbitrary malformed UTF-8/JSON frames still
exercise retirement, but complete malformed-JSON semantics and embedded engine
source coverage remain separate qualification obligations.

The same property checks bridge copies with the production engine allocator and
its original ledger. The input varies the actual guest string. It measures the
engine's temporary UTF-8 view, then verifies that only the returned destination
bytes retain credit after that view retires. A reservation on the same ledger
exhausts destination credit after successful engine startup; when the view itself
needs bytes, it also checks refusal with no view credit. Every refusal retains the
original typed budget cause, and dropping the engine releases its original bytes.
This subsumes the former standalone bridge example, whose default engine allocator
could not check temporary-view custody. The startup-allocation-prefix test keeps
its separate construction and partial-initialization obligation.

Apply `mutations/code_mode_release_bridge_credit.patch` to retire destination
credit before returning its live buffer. The unchanged property must reject it at
`bridge bytes must retain the original credit after the engine view retires`.
This mutation changes the production copy boundary and adds no allocator or
parallel accounting model. Restore it before collecting original coverage.
