# Native child merge custody

`agents/<reference>` and a canonical child AgentPath name the same fork. The
shared resolver admits only direct children for diff, merge and discard. The
native owner resolves that identity before selecting locks; either merge spelling
holds parent and child locks and refuses a claimed session or pending input.

Run `node rust/proofs/qualify.mjs native-child-merge-custody`.
The retained mutation restores the owner's prefix-only selection. The unchanged
production-owner control must reject it while an actual child process remains
alive. Restored source must refuse both spellings, reject non-child paths, and
merge captured children across two roots and a nested fork sharing a checkout.
The three native host adapters use their original captured completion inputs.
This does not qualify installed host binaries or detached writer quiescence.

No new wait, task, mount, durability barrier or traversal is introduced. Address
validation takes work proportional to the supplied path, with constant auxiliary
state and no scan over agents or history. A canonical merge now takes the same
two existing owner locks and session check as its local alias, rather than
bypassing child custody. A running session is refused immediately; the caller
waits through the existing native completion mechanism before merging.

The retained `native_child_reference` fuzz target covers arbitrary branch
operands, alias equivalence and non-child refusal. Its inverse shared-resolver
patch is `native_child_branch_parent.patch`.
