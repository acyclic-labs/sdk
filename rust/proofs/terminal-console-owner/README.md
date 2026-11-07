# Native terminal ownership from the ordinary test entry

`real_terminal_frame_cost` must run its real frame and region syscall oracles
through the ordinary test entry even when its Windows parent has no console.
That parent invokes the existing `scripts/test-terminal-signals.ps1` host. The
unchanged host creates the ConPTY, re-enters the same test executable and exact
test name, requires its original report and successful exit, and retires its
native process and output owners. The report path distinguishes that native
child from its consoleless parent; it does not bypass any measurement.

The host script is embedded unchanged and materialized below one owned temporary
root. This keeps the original script available when the compiled test is moved
to another machine. There is one ConPTY implementation. Product work is
unchanged. Test preparation adds one temporary root, one scripts directory, one
script write/copy and one PowerShell host launch. The existing host already
provides the one native child, process wait and output retirement. No polling,
timeout extension or second console lifecycle is introduced.

The retained inverse was generated from Git and restores the test source exactly
to `179c3069c37cb1fa8f06ae2bbe72f65325075730`. Apply it only for the original
negative control. An ordinary consoleless Windows invocation must fail to open
the missing original console; a compile error, skipped test or zero selected
tests is not evidence. Reversing the inverse must pass the same ordinary entry,
including native frame/region oracles and the host's original exit/report checks.
Actual original/restored Windows qualification remains required.

```sh
git apply --check rust/proofs/terminal-console-owner/original-entry.patch
git apply rust/proofs/terminal-console-owner/original-entry.patch
git diff --exit-code 179c3069c37cb1fa8f06ae2bbe72f65325075730 -- rust/crates/harness-tui/tests/terminal_frames.rs
cargo nextest run --locked -p acyclic-harness-tui --features tui --test terminal_frames -E 'test(=real_terminal_frame_cost)'
# Require the original native missing-console failure on Windows.
git apply -R rust/proofs/terminal-console-owner/original-entry.patch
cargo nextest run --locked -p acyclic-harness-tui --features tui --test terminal_frames -E 'test(=real_terminal_frame_cost)'
# Require exactly one ordinary entry to pass, without skips.
```
