# Original native task and persistence custody

Run `node rust/proofs/qualify.mjs native-custody`. Nine retained production
mutations challenge the same original task, worker and local-object paths:

| Mutation | Original obligation |
| --- | --- |
| [shared error](../shared-completion-error/mutation.patch) | Every observer retains the same operation error allocation. |
| [typed cancellation](../task-cancellation-cause/mutation.patch) | Cancellation retains its scheduler cause. |
| [typed completion](../shared-completion-cause/mutation.patch) | Shared completion retains the original nested cause. |
| [physical close](../objects-close-result/mutation.patch) | Both storage barriers propagate their original failure after worker completion. |
| [worker panic](../persistence-panic/mutation.patch) | A lost worker ends its wait with the original typed failure. |
| [wait retirement](../persistence-wait-retirement/mutation.patch) | A terminal producer retires its diagnostic wait. |
| [caller drop](../persistence-caller-custody/mutation.patch) | An accepted physical body outlives its observer. |
| [dependency observation](../persistence-reentry/mutation.patch) | A worker cannot await another job on its own service. |
| [dependency submission](../persistence-submission/mutation.patch) | Invalid submission drops its body before returning, even with a live runtime. |

Each negative selects one existing property and requires its named failure.
Exact restoration precedes the next mutation. One restored invocation selects
the original native properties, the seeded executor's same custody property,
and the real LocalObjects close/reopen property. These are production tasks,
actual persistence workers, actual local logs and their original event producers;
there is no parallel custody model or hand-written external fixture.

The unique oracle additions check retained typed causes and producer retirement
before any observer poll, keeping the same source events and storage operations.
Production syscalls, durability barriers, round trips, allocations and copies
are unchanged. Failure and cancellation must resolve through the original task
or worker event; no timer or readiness probe is introduced. Complete deployment
topology, other storage backends and platform acceptance remain separate.
