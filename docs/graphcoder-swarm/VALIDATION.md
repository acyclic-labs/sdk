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
records with evidence that names the suite, descriptor digest, execution kind,
and artifact digests. The receipt also records the source commit, worktree,
branch, pinned base, clean/unmerged state, suite transcript digests, and fresh
distributable digests.

Compilation is a separate execution kind. It may prove that code targets a
platform, but it cannot satisfy a native, PTY, package, or WASM runtime row.
The final gate also hashes every listed artifact from disk and rejects stale or
missing files, so evidence cannot be satisfied by a filename or an earlier
checkpoint alone.

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

Any failure, skip, flaky result, missing case, missing evidence, stale artifact,
or compile-only substitution keeps the receipt non-final.
