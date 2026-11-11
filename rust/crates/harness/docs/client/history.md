# Committed history controls

History is a projection of the original conversation and volume generations.
A settled conversation entry and its version-two `HistoryCheckpoint` descriptor
are published together. The descriptor names exact retained project generations
and a direct model-context cut. Retention uses structural sharing in the volume
owner.

`AgentCommand::History` returns compact summaries, newest first. An initial page
seeks directly to the current tail; it does not walk older pages. Return `next`
unchanged to read older summaries under that same opening cut. New appends are
visible through a fresh initial query; they do not extend an older continuation.
Page credit includes the original reply's wire framing. Insufficient credit is a
refusal, including when an empty response cannot fit.

A summary's `conversation` is an exclusive prefix. `Restore.at` and `Fork.at`
select the included zero-based turn, so their value is `conversation - 1`.
The selected agent and immutable turn are authority; a displayed row index is not.
The full descriptor remains at its original indexed history record. A picker
receives only the conversation cut, context tail and project-binding count.

GraphCoder's `/history` opens this bounded summary view for the current agent.
Page Down reveals retained rows before following the opaque older continuation;
Page Up scrolls to the start before opening the newest page. New committed turns
refresh the newest page after any pending read completes. Older pages keep their
original cut. Resize only wraps the retained page; Escape closes its pending
consumer. This view displays cuts and does not submit a restore or fork.

## Restoration

The caller explicitly chooses one scope:

| Mode | Project generations | Future model context |
| --- | --- | --- |
| `files` | Restore the saved project generations | Keep the current context |
| `context` | Keep current projects | Select the saved context cut |
| `both` | Restore the saved project generations | Select the saved context cut |

For `files` and `both`, query `AgentCommand::ProjectHeads` before confirmation.
Pass those observed current heads as `expected` for every project binding. Saved
generations describe the target; they are never substitutes for expected current
heads. `context` carries no expected file heads.

Restoration appends a new conversation control entry. It preserves the original
conversation and descriptors. Successful `both` publishes the new project heads,
selected context and control entry through the same conditional commit. A changed
expected head produces a conflict; it cannot partially restore the projects or
select the new context. Observe the committed result rather than inferring
completion from delivery to an inbox.

Keep the original agent, mode, turn, expected heads and request identity across
an uncertain attempt. Reuse the original prepared invocation to retry unchanged.
A definitive conflict requires a fresh current-head query and a new user decision.

Private journal volumes and session-shared volumes stay live. Restoring project
generations does not cancel children, rewind their work, undo external actions or
recover paths outside the captured volumes. A context-only selection does not
undo file changes. Shell changes belong to a checkpoint only after the original
filesystem capture has settled them into the selected generation.

## Forks

`AgentCommand::Fork` selects the original parent's exact turn and uses the shared
stream fork and volume fork composition. The child inherits the selected
conversation prefix and retained project generations. The parent keeps its later
conversation and files. The new brief is staged by the authorized host. Like
`Spawn`, a fork names the child's `role`, which the parent's role must admit; a
fresh-context role is refused, because it would inherit no conversation to select.

`AgentCommand::ForkSession` uses the owner's admitted session directory to create
an independent sibling root at the selected turn. It composes the same stream
and volume fork with a fresh inbox, private journal and actor. The new actor is
paused with zero escrow; no model runs until an explicit resume. The original
source keeps its later conversation and files. A repeated request retains the
same prepared publication; changing its source or cut under that identity is
refused.

Each settled turn retains its project generation through the shared volume
composer. Native bindings prepare their lazy fork companion at that same cut;
later history forks use the retained workspace rather than the source's current
head. Shell writes belong to that cut only after their original capture settles.
External effects and children are never rewound.

GraphCoder's `/fork` and `/rewind` open the same newest-first history chooser.
Both select a paused sibling and preserve the abandoned branch. `/unpause`
explicitly resumes it. Directory admission also permits a retained cut of an
archived source to be forked without reopening the source's closed inbox.

Inherited transcript reads prove the exact committed conversation record and
grant only its referenced files. They never grant the source's private volume.
Selection remains subject to the original admission and current revocation
checks; knowing a path conveys no authority.
