# Parent-scoped child branches

Run `node rust/proofs/qualify.mjs native-child-branch-scope` on Linux.

The existing native process-owner control runs the Codex, Claude Code and Pi
adapters with their recorded output and real shell processes. Two roots share
one checkout; each spawns `shared`, and one child also spawns `shared`. Their
writes remain in their forks until the owning parent merges them. Diff, merge
and discard resolve the caller's child without borrowing another root's branch.
Owners enter through production policy admission, ownership and recovery;
the nested owner inherits its parent's admitted policy arguments.

The retained inverse restores the actual native checkout-wide alias registration
and command routing. The unchanged control must reject that production mutation
at the second root's claim, then complete with exact original source restored.
This component qualification does not run installed native host CLIs or establish
complete platform acceptance.
