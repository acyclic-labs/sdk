# Installed lane coverage boundary

The installed package and PTY command sequences are useful transport and
terminal checks, but they do not currently prove the complete swarm contract.
The production bridge in `rust/crates/graphcoder-cli/src/main.rs` currently
dispatches session listing/start/open/resume, activity, messages, approvals,
cancellation, and file reads. Its `list_changes`, `read_change`, and
`approve_writeback` routes return `unsupported`, so the documented `changes`,
`diff`, and `writeback` commands cannot produce passing native or PTY evidence
until those routes are implemented by the owning Harness/Filesystem boundary.

The installed GraphCoder command surface also has no model-facing `fork`,
`execute`, or `wait` command. The Rust deterministic provider contains a
recursive fixture that emits child fork tool calls, but the installed terminal
sequence never selects that fixture or inspects the resulting recursive tree.
The existing stage fixture only stages a file. Consequently the installed
lanes currently omit proof of recursive root/child/grandchild work, native
subprocess admission and approval, message/wait recovery, and root writeback
reconciliation. Tooling receipts must not claim those rows from transport
success alone.

The smallest integration sequence is:

1. Use the source-bound `graphcoder-installed-swarm-e2e.mjs` driver beside
   `graphcoder-native-stage-e2e.mjs`. It sends `start_session` with the
   explicit recursive model fixture, then reads the agent tree/activity and
   message pages until root, two children, and a grandchild complete. It must
   assert child identities, direct-parent relationships, ordered messages,
   frozen prefix digests, and final workspace bytes.
2. Extend the deterministic fixture with one approved native subprocess and
   one pending root writeback. The driver must inspect the pending approval,
   resolve it through the authenticated operator path, verify exact operation
   and generation binding, and assert the durable effect and process cleanup.
3. Implement the missing typed change/read-change/writeback routes through the
   existing Harness/Filesystem APIs. Keep GraphCoder as a terminal wrapper;
   do not add a second orchestration or storage path in the package.
4. Add `wait`/resume observation to the wire boundary or expose an equivalent
   paged durable activity protocol. Capture cancellation, timeout, restart, and
   unknown subprocess outcomes in the same driver.
5. Bind the new driver to a fresh qualification-suite receipt with the native
   runtime and installed package artifacts. Only then map the relevant locked
   rows to native/package/PTY evidence.

The platform descriptor now requires that receipt as
`GRAPHCODER_INSTALLED_SWARM_RECEIPT`; `run windows` cannot complete while it is
missing or failed.

The deterministic provider remains an allowed mock model for this goal. A
fixture transport or a package-export smoke test cannot substitute for the
real Harness bridge, Filesystem effects, native process owner, or writeback
approval.
