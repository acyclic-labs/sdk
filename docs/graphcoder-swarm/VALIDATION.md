# Qualification loop

The acceptance matrix lives in `requirements.json`; it is the source of truth
for coverage. Each requirement has a stable ID, a contract, a verification
scenario, and the execution kinds that must be represented by final evidence.
Keep IDs stable when implementation details change. Add a new row when a new
contract is introduced instead of silently changing an existing row's meaning.

The validator is intentionally dependency-free and can run before the full SDK
toolchain is installed:

```text
node scripts/graphcoder-qualification.mjs matrix-check
node scripts/graphcoder-qualification.mjs render docs/graphcoder-swarm/requirements.json docs/graphcoder-swarm/REQUIREMENTS.generated.md
node scripts/graphcoder-qualification.mjs receipt-template docs/graphcoder-swarm/requirements.json .qualification/graphcoder-receipt.json
node scripts/graphcoder-qualification.mjs receipt-check docs/graphcoder-swarm/requirements.json .qualification/graphcoder-receipt.json
```

The template is deliberately incomplete. It records every matrix ID as
`pending` and cannot pass the final gate. A qualification run replaces those
records with evidence that names the suite, its on-disk descriptor and
transcript digests, the execution kind, the execution window, and the exact
artifacts consumed by that suite. The receipt also records the source
commit, worktree, branch, pinned base, clean/unmerged state, suite transcript
digests, and distributable build provenance. The validator compares those claims
with the current Git checkout and hashes the referenced files itself.

Compilation is a separate execution kind. It may prove that code targets a
platform, but it cannot satisfy a native, PTY, package, or WASM runtime row.
The final gate requires each listed artifact to name the qualified source
commit and tree, a build ID, and a timestamp, then hashes it from disk. Stale or
missing files are rejected, so evidence cannot be satisfied by a filename,
boolean freshness flag, or an earlier checkpoint alone.

The intended loop is:

1. Run focused contract tests for the earliest unmet matrix row.
2. Run its black-box effect scenario against the real local providers and the
   deterministic model fixture.
3. Record the suite transcript and descriptor digest, then attach the case to
   the receipt.
4. Build fresh artifacts and run the installed-package and PTY lanes against
   those artifacts.
5. Run `receipt-check --final` only after every row is passed and every required
   execution kind is present.

For the GraphCoder application lane, the installed artifact must be driven
through the public terminal entrypoint and the same command sequence must be
run through both headless and Windows PTY transports. The PTY descriptor must
identify the exact packaged GraphCoder artifact and the transcript must retain
the lifecycle, approval, lazy file/diff, and cancellation markers. The bridge
used by that run must be the durable local Harness runtime; the deterministic
fixture remains a separate mock lane and cannot satisfy native or PTY rows.
The runtime bridge is injected through `HarnessGraphCoderTransport`, so this
qualification runner does not create a second session store or orchestration
path in GraphCoder.

Use `scripts/graphcoder-qualification-suite.mjs capture CONFIG.json` at the
same boundary for each native, WASM, PTY, and packaged run. The configuration
names the exact executable and argument vector, working directory, explicit
environment allowlist, expected exit code, and every artifact consumed by the
run. The utility refuses stale source provenance, artifacts built after the
suite began, duplicate or changing artifacts, shell execution, and missing
files. It records the command descriptor, artifact hashes, execution window,
and combined transcript in a suite record that can be copied into the locked
receipt. A nonzero process outcome produces `status: failed` and a nonzero
utility exit status, preserving the failure for the gate.

Any failure, skip, flaky result, missing case, missing evidence, stale artifact,
or compile-only substitution keeps the receipt non-final.
