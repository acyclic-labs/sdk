# Portable platform gates

`docs/graphcoder-swarm/platform-gates.json` is the executable inventory for
the local GraphCoder qualification. It names the exact argv vector for each
Rust Harness, generated-binding, TypeScript, provider, Filesystem/plugin,
package, and dependency-boundary gate. The inventory complements the locked
requirements matrix; it does not replace or duplicate that matrix.

The descriptor also contains `qualification_lanes` for artifact-dependent
coverage that cannot be represented by a static platform argv. The validator
requires every declared driver and scenario to exist before a gate can run.
The installed native-stage lane names the real `graphcoder-runtime` artifact;
the headless and PTY lanes require an installed package, process-ownership
evidence, and lazy-observation markers. The Windows PTY lane additionally
requires approval, cancellation, and writeback markers and records the native
WinPTY driver. These lanes reject mock fixtures. The Filesystem/plugin lane
explicitly runs ignored fork/join tests and records its support-test skip, so
an ignored test invocation cannot silently become a complete package check.

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

`graphcoder-platform-gates.mjs run windows` consumes installed-lane receipts
through environment variables. Capture them in this order from a clean
qualified worktree, using fresh artifacts from the same source commit and
tree:

1. Build `acyclic-graphcoder-cli` with `cargo build -p acyclic-graphcoder-cli
   --locked`, capture `graphcoder-native-stage-e2e.mjs` with
   `graphcoder-native-stage-config.mjs`, and export its record as
   `GRAPHCODER_NATIVE_STAGE_RECEIPT`.
2. Build and pack GraphCoder and Filesystem, then capture the real Harness
   bridge through `graphcoder-production-entrypoint.mjs` once as `native` and
   once as `package`. Export the records as
   `GRAPHCODER_HEADLESS_NATIVE_RECEIPT` and
   `GRAPHCODER_HEADLESS_PACKAGE_RECEIPT`.
3. Run the same installed command sequence through
   `graphcoder-production-pty.mjs` and the native WinPTY driver on Windows;
   export its record as `GRAPHCODER_PTY_RECEIPT`.
4. Run `graphcoder-installed-transport-faults.mjs` against the extracted
   package with descendant cleanup required; export its record as
   `GRAPHCODER_TRANSPORT_FAULTS_RECEIPT`.
5. Run `node scripts/graphcoder-platform-gates.mjs run windows` with those
   receipt variables. The runner validates every producer record, descriptor
   command, source identity, execution kind, status, and fresh artifact before
   reporting platform qualification as complete.

The production bridge may use a deterministic mock model provider because the
goal requires mocked models. A mock transport or fixture bridge cannot satisfy
installed native, package, or PTY receipts; those lanes still exercise the
durable Harness, real filesystem effects, and native process boundary.
