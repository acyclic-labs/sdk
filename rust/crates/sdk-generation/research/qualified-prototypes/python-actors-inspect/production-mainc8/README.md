# Current mainc8 Python evidence

This directory records the Python evidence produced from the current `sdkgen-actors-c8-minimal` checkout. It is source-bound to the producer manifest in `producer-manifest-mainc8.json`; the older `production-actual03bb` receipts remain historical and are not retagged.

The maintained UniFFI 0.31 generator patch is paired with the nominal custom-type and frozen-record patch. The current config declares Rust-backed validators for `ActorId`, `CodeSha256`, `PositiveU64`, and the true-only `CurrentHeadMarker`. Generated output is kept in the external `Q:\sdk\work\python-actors-c8-generated-current` directory.

`typing/positive.py` covers all nineteen semantic roots, nested cursor/current-head values, u64 boundaries, optional presence, nominal brands, and frozen records. `typing/negative.py` contains eight deliberate brand, presence, oneof, and readonly errors. The manifest points to the actual mypy and Pyright logs.

The fresh Windows wheel was installed into an isolated venv and exercised against task-owned TLS fixtures. The all-eight request runner passed; three real pending `asyncio.Task.cancel()` iterations observed server abort cleanup and a zero continuation-handle map. Linux WSL and macOS ivar rebuilds still need to be run against this exact producer identity before the cohort is cross-platform qualified.
