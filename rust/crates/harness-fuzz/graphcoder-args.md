# Shared CLI argument corpus

These are authored user arguments, not provider responses or external command
output. Each file contains UTF-8 arguments separated by NUL, excluding argv[0].
Empty input means no arguments. No seed contains credentials.

The Windows adapter also accepts an FF 00 prefix followed by little-endian UTF-16
arguments separated by zero units, preserving unpaired surrogate units. Unix
inputs preserve invalid UTF-8 bytes. The adapter and the original 36 corpus files
come from source e3098d5f961610cd4b1b55e3ab698cb4e68ab474 at
`rust/crates/harness-fuzz/corpus/graphcoder_args`; all original file bytes are
unchanged here. `accepted-family-6` and `accepted-family-7` are authored one-byte
selectors for the run and attach families.

The existing `cli_args` target invokes the shipping ingress, clap parser and
`prepare_input` in `plugin/src`. It opens no runtime, config, provider or output
file. Accepted results retain an input producer and operation identity, one
prompt owner, compatible output mode, distinct output destinations and the
explicit attach selection. Native workspace ownership requires explicit storage
and excludes agent inputs. Serving requires either no TLS identity or its complete
certificate, key and URL. Generated canonical reopen paths must survive admission
without consulting ambient selection.

The first byte modulo eight selects a generated accepted family: explicit
execution, native workspace ownership, loopback serving, TLS serving, prompt,
JSONL input, run or attach. Rejecting one of these supported families fails the
same oracle used for arbitrary accepted input.

The exec positional task has its own clap identifier, separate from the global
prompt option. The empty input and `accepted-family-0` exercise that supported
execution; sharing their identifiers would falsely report two supplied tasks.

The existing control driver removes actual production operation, host storage,
TLS key, prompt/input and distinct task-identifier guards in turn. Each selected defect must fail
its unchanged semantic assertion; the driver restores the original bytes before
replaying the retained corpus. Run from the repository root:

```
python3 rust/crates/harness-fuzz/graphcoder-args-control.py cli_args
```

Instrumentation reports reached edges for the replay, not complete source branch
coverage or a sustained fuzz campaign. The Windows native-unit adapter still
requires Windows qualification; Linux evidence does not cover it. Ordinary unit
coverage for host storage, TLS, command normalization and input admission remains.
The deleted retired-entrypoint example's unknown-option check survives in
`headless_modes_use_explicit_jsonl_identity_and_reject_mixed_formats`, which also
retains valid resume admission and extra-positional rejection.
