# Python Actors maintained UniFFI qualification prototype

This directory retains the minimal maintained UniFFI 0.31 Python source patch,
its external producer closure, and the consumer used for remote qualification.
The full vendor copy used during diagnosis is intentionally absent. No binary
artifact is checked into this research directory.

The primary closure is `minimal-patched-generator-receipt.json`. It records the
pinned crates.io archive and embedded source files, the five patched generator
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

The fresh checker outputs show mypy 1.17.1 and Pyright 1.1.404 passing the
positive fixture and both checkers rejecting the six deliberate negative cases.
The generator patch now emits static narrow constructor classes for data enum
variants, so the `CURRENT_HEAD` diagnostics are gone while the runtime nested
constructors remain unchanged. The runtime and cross-platform install results
remain recorded as external receipts until an install harness emits a
source-bound producer manifest beside the patched wheel.

`installed-all8-remote-conformance.py` is the remote receipt producer. It
appends each operation only after its response assertions pass and appends
`remote`, `service_error`, and `pre-cancelled` only after their checks pass.
When `ACYCLIC_PENDING_FIXTURE_OPTIONS` is supplied, it additionally appends
`in-flight-task-cancellation` and `server-abort-cleanup` only after a pending
request is cancelled through a real `asyncio.Task` and the server observes the
stream close. When `ACYCLIC_QUALIFICATION_RECEIPT` is set, the script requires
`ACYCLIC_PRODUCER_MANIFEST` and the wheel path, measures the wheel before and
after execution, and writes the source-bound schema consumed by the
language-package model:

```json
{"schema":"acyclic.language-package.qualification/v1","status":"PASS",
 "operations":["..."],"checks":["..."],
 "source_revision":"...","source_inventory_sha256":"...",
 "toolchain":{"uniffi_bindgen":"0.31.0","uniffi_source_sha256":"...",
               "python_patch_sha256":"..."},
 "artifacts":{"wheel":{"path":"...","sha256":"...","bytes":0}}}
```

The arrays and status are produced by the executed run; the script does not
accept caller-supplied success or scope claims. The producer manifest is an
install-harness output and must contain the exact source revision, source
inventory hash, UniFFI version, uniffi source hash, and exact Python patch
hash. The install
check belongs to the harness that actually installs the wheel and must be
emitted there before the root source-bound model consumes the receipt.

The older `all8-qualification-receipt.json` and
`cross-platform-qualification-receipt.json` are historical context. Their
claims are not substituted for the measured patched-generator closure.

The source-bound installed receipts are retained under `installed-receipts/`.
Windows, Linux/WSL, and macOS/ivar each report the same eight operations and
the checks `remote`, `service_error`, `pre-cancelled`,
`in-flight-task-cancellation`, and `server-abort-cleanup`. Each receipt binds
the measured wheel identity to the same producer manifest and is emitted only
after the wheel hash is unchanged before and after execution.

