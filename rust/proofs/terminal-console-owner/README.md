# Native terminal ownership from the ordinary test entry

`real_terminal_frame_cost` must run its real frame and region syscall oracles
through the ordinary test entry even when its Windows parent has no console.
That parent invokes the existing `scripts/test-terminal-signals.ps1` host. The
host creates the ConPTY, re-enters the same test executable and exact test name,
requires its original report and successful exit, and retires its native process
and output owners. The report path distinguishes that native child from its
parent; it does not bypass any measurement.

The host script is embedded and materialized below one owned temporary root.
This keeps the script available when the compiled test moves to another machine.
There is one ConPTY implementation. Product work is unchanged. Test preparation
adds one temporary root, one scripts directory, one script write/copy and one
PowerShell host launch. The existing host provides the native child, process
wait and output retirement. No polling or timeout extension is introduced.

The Git-generated `original-entry.patch` restores the test source exactly to
`179c3069c37cb1fa8f06ae2bbe72f65325075730`. The retained `consoleless.patch`
establishes the real missing-console precondition: Windows `FreeConsole`
detaches only the test parent's inherited console before the ordinary entry.
The existing report path leaves its hosted native child attached. This fixture
creates no process owner or console, and changes no frame or region oracle.
An enclosing native process owner may supply a console, so invoking the original
entry without this fixture does not qualify the missing-console failure.

Verify the original source before applying the fixture. Require exactly one
selected native test, no selected skips, and an original missing-console error.
A fixture setup refusal, compile error or zero selected tests is not evidence.
Remove the fixture, reverse the original-entry inverse, then apply the
same detached-parent fixture again. Require the original frame/region oracles,
report acknowledgement, successful exit and process retirement. Remove the
fixture afterward. Ordinary contributor checks use the unmodified source.

```sh
git apply --check rust/proofs/terminal-console-owner/original-entry.patch
git apply rust/proofs/terminal-console-owner/original-entry.patch
git diff --exit-code 179c3069c37cb1fa8f06ae2bbe72f65325075730 -- rust/crates/harness-tui/tests/terminal_frames.rs
git apply rust/proofs/terminal-console-owner/consoleless.patch
cargo nextest run --locked -p acyclic-harness-tui --features tui --test terminal_frames -E 'test(=real_terminal_frame_cost)'
# Require the original native missing-console failure on Windows.
git apply -R rust/proofs/terminal-console-owner/consoleless.patch
git apply -R rust/proofs/terminal-console-owner/original-entry.patch
git apply rust/proofs/terminal-console-owner/consoleless.patch
cargo nextest run --locked -p acyclic-harness-tui --features tui --test terminal_frames -E 'test(=real_terminal_frame_cost)'
# Require exactly one ordinary entry to pass, without selected skips.
git apply -R rust/proofs/terminal-console-owner/consoleless.patch
```
