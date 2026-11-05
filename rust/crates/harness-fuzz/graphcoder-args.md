# GraphCoder argument corpus

These are authored user arguments, not recorded provider responses or external
command output. A file contains UTF-8 arguments separated by NUL, excluding
argv[0]. Empty input means no arguments. Mutations also exercise native invalid
UTF-8 on Unix and native UTF-16 units on Windows. No seed contains credentials.

Windows also admits a `FF 00` prefix followed by little-endian UTF-16 arguments
separated by zero *units*. This preserves zero bytes inside units such as D800
and DC00. `native-d800` and `native-dc00` retain those authored OS inputs; the
readable UTF-8 corpus uses its original encoding.

The target includes the shipping `graphcoder/args.rs` and invokes its clap parser
and `prepare_input`. It opens no runtime, config, provider or output file.
Accepted results must retain one input producer and operation identity, one
prompt owner, compatible output mode and distinct destinations. Accepted native
workspace ownership requires explicit storage and excludes agent inputs; serving
requires either no TLS identity or its complete certificate, key and URL.
The first byte modulo six selects a generated accepted family: explicit execution,
native workspace ownership, loopback serving, TLS serving, prompt or JSONL input.
Each family must parse and normalize, so rejecting a supported family fails the
oracle. The six `accepted-family-*` seeds exercise each selection.

`exec-missing-operation` reaches the post-clap normalization guard. The kept
negative control removes that actual production guard. Additional controls remove
the actual native storage requirement, TLS key requirement and prompt/input
conflict annotations. Each requires its unchanged semantic assertion to fail,
restores the source, then replays the corpus with production admission. Run from
the repository root:

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
