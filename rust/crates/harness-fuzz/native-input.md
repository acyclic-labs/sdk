# Native stdin and task admission

The installed plugin and `native_input` target include the same `input.rs`, which
admits stdin JSON and delegates selected task identities to the SDK AgentPath
decoder before any workspace lookup. The authored corpus contains user inputs,
not external command output. Generated valid JSON and original typed AgentPath
values must also be admitted; rejecting everything fails the oracle.

The byte-bound property supplies valid JSON below, at and above the existing
65,536-byte boundary. The former spaces-only example was invalid JSON at every
size and could not distinguish a removed bound check. The kept production guard
mutation must fail the unchanged bound assertion. That subsumed example is
deleted; real binary, OS pipe and lifecycle controls are separate.

```
python3 rust/crates/harness-fuzz/graphcoder-args-control.py native_input
```

No runtime, workspace, settings or host command is opened by the target. Read
errors, interrupted/fragmented OS pipes, installed host stdout and native settings
remain distinct validation obligations. Corpus replay is not complete source
branch coverage or a fuzz campaign.
