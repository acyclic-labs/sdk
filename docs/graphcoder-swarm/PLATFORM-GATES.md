# Portable platform gates

`docs/graphcoder-swarm/platform-gates.json` is the executable inventory for
the local GraphCoder qualification. It names the exact argv vector for each
Rust Harness, generated-binding, TypeScript, provider, Filesystem/plugin,
package, and dependency-boundary gate. The inventory complements the locked
requirements matrix; it does not replace or duplicate that matrix.

The runner never invokes a shell and runs with a filtered environment. It
records the canonical worktree root, source commit/tree, a digest of the
actual tracked Rust source bytes, and a qualification-source digest covering
every tracked working-tree byte. The shared source-fence helper keeps the
inventory NUL-safe and hashes newline-bearing paths through argv. It records
the manifest digest, command digest, suite ID, and transcript digest for each
executed gate. Cargo gates receive a target directory keyed by that source
identity, and reject output directories already claimed by another source
root. Credentials and secret-looking environment variables are excluded. Build
and packaging gates use the allowlisted toolchain environment policy;
host-executed runtime gates receive only the runtime policy. A gate is failed
when its command exits nonzero, times out, or the source changes while the run
is in progress.

The native Harness gate enables `filesystem-local`, `native-process-tree`, and
`test-support` together, so physical local-storage and process-tree probes are
executed in the same source-bound lane rather than silently reduced to memory
or compile coverage.

Use these commands from the qualified worktree:

```text
node scripts/graphcoder-platform-gates.mjs list
node scripts/graphcoder-platform-gates.mjs check windows
node scripts/graphcoder-platform-gates.mjs run windows
```

The platform argument is `windows`, `linux`, or `macos`; when omitted, the
runner selects the current host. A requested platform that differs from the
host is rejected, so cross-compilation cannot be reported as platform
execution. The `run` command writes one transcript and one source-bound record
per gate beneath `.qualification/platform-gates/`, plus `summary.json`. Each
record includes a suite descriptor digest, helper provenance digests for the
manifest/runner, any invoked qualification helper, and the transcript, plus
`qualification_artifacts` for artifacts
explicitly declared as produced by that gate. The locked source/test gates do
not produce distributable SDK artifacts, so their qualification-artifact lists
are empty; package and PTY artifact digests come from the packaging lane below.
Each record also carries `execution_evidence`: compile gates are labelled
`compile-only`, while native, package, and other runtime gates require an
observed host process exit on the verified host platform. A compile result is
therefore never presented as native execution evidence.
Every produced artifact digest is generated after execution and carries the
gate command digest plus the canonical source identity; caller-provided source
labels are not trusted as provenance. The producer observation records the
actual exit, signal, error, and post-dispatch filesystem observation. Declared
generated outputs are exact relative untracked paths: the runner records their
pre-dispatch bytes, removes stale files, and requires a newly created
post-dispatch file before recording the artifact. A preexisting valid binary
therefore cannot satisfy a no-op gate. The producer fence is followed by a
second post-suite source fence, so evidence collection cannot silently detach
from the gate source.
Child processes are placed in one shared owned process tree helper (Unix
process group or the Windows descendant-aware taskkill fallback). Timeout,
signal, exception, and normal parent exit all terminate the tree before
captured readers can remain open. Termination has a bounded wait and records
an explicit uncertain outcome when the owner cannot prove closure. The native Rust lane remains the authoritative
Job Object owner when it is available; this JavaScript runner is the portable
gate boundary. The Windows native lane remains the authoritative evidence for
actual Windows execution; compile-only checks must be recorded separately.
Source snapshots are byte fences for the recorded boundaries; they do not claim
to detect an edit that mutates and restores identical bytes between snapshots.

Installed package and PTY evidence continues to use
`graphcoder-qualification-suite.mjs`, because those suites consume fresh
archives and host bridge artifacts whose paths are created by the packaging
lane. Their suite descriptors should reference the same qualified commit/tree
and be included in the final receipt alongside these platform-gate records.
