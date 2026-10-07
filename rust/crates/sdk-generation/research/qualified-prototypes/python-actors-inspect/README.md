# Python Actors maintained UniFFI qualification prototype

This directory retains the minimal maintained UniFFI 0.31 Python source patch,
its external producer closure, and the consumer used for remote qualification.
The full vendor copy used during diagnosis is intentionally absent. No binary
artifact is checked into this research directory.

The primary closure is `minimal-patched-generator-receipt.json`. It records the
pinned crates.io archive and embedded source files, the three patched generator
source hashes, the generated Python module, native libraries, wheels, checker
output hashes, and the current Rust facade checkout hashes. The external
outputs are retained qualification evidence; the receipt marks them
`source_binding: UNBOUND` because a release claim requires rerunning the
producer against the recorded current source snapshot. Run
`verify-minimal-patched-generator-receipt.ps1` to recompute the recorded
external hashes and byte counts.

The maintained patch is `uniffi-python-typing.patch`, reproduced by
`run-uniffi-python-typing-patch.sh` from the pinned `uniffi_bindgen` 0.31.0
archive. It adds the generated public enum value union while preserving the
runtime nested constructors and dynamic reparenting. Source revision, archive,
license, and changed-file provenance are in
`uniffi-python-typing-patch-provenance.md`.

The retained checker outputs show mypy 1.17.1 passing the positive fixture and
both mypy and Pyright rejecting the six deliberate negative cases. The retained
Pyright positive output contains two `CURRENT_HEAD` constructor diagnostics;
this prototype therefore does not claim a zero-diagnostic Pyright positive
run. The runtime and cross-platform install results remain recorded as
external receipts, with no producer log retained beside the patched Linux and
macOS wheels.

`installed-all8-remote-conformance.py` is the actual remote receipt producer.
It appends each operation only after its response assertions pass and appends
`remote`, `service_error`, and `cancellation` only after their checks pass.
When `ACYCLIC_QUALIFICATION_RECEIPT` is set, it writes the minimal producer
schema consumed by the language-package model:

```json
{"schema":"acyclic.language-package.qualification/v1","status":"PASS",
 "operations":["..."],"checks":["..."],"fixture_options_sha256":"..."}
```

The arrays and status are produced by the executed run; the script does not
accept caller-supplied success or scope claims. The install check belongs to
the harness that actually installs the wheel and must be emitted there before
the root source-bound model consumes the receipt.

The older `all8-qualification-receipt.json` and
`cross-platform-qualification-receipt.json` are historical context. Their
claims are not substituted for the measured patched-generator closure.

