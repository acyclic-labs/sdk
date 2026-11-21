# Native terminal ownership from the ordinary test entry

`terminal_frames::real_terminal_frame_cost` must run its real frame and region syscall oracles
through the ordinary test entry even when its Windows parent has no console.
That parent re-enters the same test executable and exact test name as a
native process tree, whose child the runtime attaches to its one pseudoconsole
(`native-runtime`'s `windows_create/console.rs`). It requires the child's
report and successful exit; the tree retires the native process and output
owners. The report path distinguishes that native child from its parent; it
does not bypass any measurement. Test preparation adds one temporary root and
one process tree. No polling or timeout extension is introduced.

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
cargo nextest run --locked -p acyclic-harness-tui --test main -E 'test(=terminal_frames::real_terminal_frame_cost)'
# Require the original native missing-console failure on Windows.
git apply -R rust/proofs/terminal-console-owner/consoleless.patch
git apply -R rust/proofs/terminal-console-owner/original-entry.patch
git apply rust/proofs/terminal-console-owner/consoleless.patch
cargo nextest run --locked -p acyclic-harness-tui --test main -E 'test(=terminal_frames::real_terminal_frame_cost)'
# Require exactly one ordinary entry to pass, without selected skips.
git apply -R rust/proofs/terminal-console-owner/consoleless.patch
```
