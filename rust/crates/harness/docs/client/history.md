# Committed history controls

History is a projection of the original conversation and volume generations.
A settled conversation entry and its version-one `HistoryCheckpoint` descriptor
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

## Forks and current integration limits

`AgentCommand::Fork` selects the original parent's exact turn and uses the shared
stream fork and volume fork composition. The child inherits the selected
conversation prefix and retained project generations. The parent keeps its later
conversation and files. The new brief is staged by the authorized host. Like
`Spawn`, a fork names the child's `role`, which the parent's role must admit; a
fresh-context role is refused, because it would inherit no conversation to select.

Complete lazy-workspace restoration still requires retained source, overlay and
shadow state to join the authored generation in the original filesystem commit.
The current selected-generation fork can fail with `spawned project has no
prepared fork` when its native view needs a lazy companion that was not retained.
An authored-generation-only result does not establish complete lazy-workspace
restoration or captured-shell settlement.

The current fork control is consumed through the parent's inbox. An archived
parent therefore refuses the request. Forking any retained turn of an archived
session remains unfinished. Selected descendant observation also requires the
original admission and current revocation checks; selecting a path does not grant
access to it.

Generation sharing alone does not establish constant-cost cold admission: an
unretained publication proof can still require a complete source proof walk.
Reusable proof custody, complete lazy-state publication and archived fork
composition must be resolved before claiming those guarantees.
