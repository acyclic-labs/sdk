# GraphCoder argument corpus

These are authored user arguments, not recorded provider responses or external
command output. A file contains UTF-8 arguments separated by NUL, excluding
argv[0]. Empty input means no arguments. Mutations also exercise native invalid
UTF-8 on Unix and native UTF-16 units on Windows. No seed contains credentials.

Windows also admits a `FF 00` prefix followed by little-endian UTF-16 arguments
separated by zero *units*. This preserves zero bytes inside units such as D800
and DC00. `native-d800` and `native-dc00` retain those authored OS inputs; the
readable UTF-8 corpus uses its original encoding.

The one `graphcoder_args` target uses the shipping plugin's public parser and
native ingress, including `prepare_input`. Its `graphcoder_args` corpus retains
all 38 original CLI seeds, with duplicate seeds stored once. It opens no runtime,
config, provider or output file.
Accepted results must retain one input producer and operation identity, one
prompt owner, compatible output mode and distinct destinations. A generated
explicit execution must also succeed, so rejecting everything fails the oracle.
Generated valid inputs also cover native workspace ownership, plain and TLS
serving, prompt and JSONL input, `run`, and attachment. Explicit root and child
reopen paths must preserve their identity without consulting ambient selection.

The binary requires the `runtime` feature. An all-target check that enables only
the `graphcoder` feature does not compile it. Compile the selected binary and
replay its retained corpus with:

```
cargo check --locked -p acyclic-harness-fuzz --bin graphcoder_args --no-default-features --features runtime
cargo run --locked -p acyclic-harness-fuzz --bin graphcoder_args --no-default-features --features runtime -- -runs=0 rust/crates/harness-fuzz/corpus/graphcoder_args
```

`exec-missing-operation` reaches the post-clap normalization guard. The kept
negative control removes that actual production guard, requires the unchanged
oracle's exact identity assertion to fail, restores the source, then replays the
corpus with production admission. Run from the repository root:

```
python3 rust/crates/harness-fuzz/graphcoder-args-control.py
```

LibFuzzer instrumentation reports reached edges for this replay. A replay is not
a fuzz campaign or complete source branch coverage. Source coverage can be
collected separately with the existing cargo-fuzz coverage command. Windows's
native-unit adapter still needs a Windows qualification; Linux evidence does
not cover it. [JSONL request decoding](graphcoder-input.md), persisted cursors,
output schema admission, native config and installed host command parsing are
separate boundaries.
