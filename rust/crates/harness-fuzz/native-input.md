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

The same target calls the production command catalog decoder with the installed
`commands.json` and derived caller inputs. It checks bounded admission, normalized
names and aliases, descriptor preservation, argument shapes, confirmation policy,
duplicate names, invalid descriptions and unknown fields. Control 7 removes the
actual argument-shape guard; its unchanged refusal assertion must detect the
mutation before the restored catalog corpus can pass.

The stdin oracle uses a real OS pipe, EOF from its producer, fragmented writes and
a write-only file's original OS read error. It does not execute a workspace or
host action. Full binary dispatch, owner effects, installed host stdout and native
settings remain separate obligations. Corpus replay is not complete source branch
coverage or a new fuzz campaign. Each manifest records only its qualified source;
the catalog extension requires its own mutation and restored-corpus result.
