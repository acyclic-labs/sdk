# Locked matrix gap audit

The authoritative row definitions and required execution kinds remain in
`requirements.json`. Every row below is still open because this checkout has
no final source-bound receipt. Only a fresh receipt with named assertions,
executed transcripts, source and descriptor digests, fresh artifacts, and no
failed, skipped, flaky, missing, stale, or fixture-only evidence can close the
matrix. Each locked ID is listed exactly once.

| Current state | Matrix IDs | Required next evidence |
|---|---|---|
| open | SCOPE-01, SCOPE-02, SCOPE-03, SCOPE-04, SCOPE-05, SCOPE-06, INPUT-01, INPUT-02, INPUT-03, INPUT-04, INPUT-05, INPUT-06, INPUT-07, FORK-01, FORK-02, FORK-03, FORK-04, FORK-05, FORK-06, FORK-07, FORK-08, FORK-09, EFFECT-01, EFFECT-02, EFFECT-03, EFFECT-04, EFFECT-05, EFFECT-06, EFFECT-07, EFFECT-08, WORK-01, WORK-02, WORK-03, WORK-04, WORK-05, WORK-06, API-01, API-02, API-03, API-04, API-05, API-06, API-07, LIMIT-01, LIMIT-02, HOST-01, HOST-02, CLI-01, CLI-02, CLI-03, CLI-04, CLI-05, LOAD-01, LOAD-02, BIND-01, BIND-02, E2E-01, E2E-02, E2E-03, E2E-04, E2E-05, E2E-06, FAULT-01, FAULT-02, QUAL-01, QUAL-02, QUAL-03, QUAL-04 | Re-run the final source through native, compile, WASM, package, PTY, transport-fault, and receipt-validation lanes in the order documented by `PLATFORM-GATES.md`. |
