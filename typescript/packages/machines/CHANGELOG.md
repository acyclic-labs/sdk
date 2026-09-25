# @acyclic-labs/machines changelog

## 0.1.4 - 2026-09-25

- Aligns machine clients with the qualified SDK 0.1.4 release.

## 0.1.3 - 2026-09-25

- Add `forkMachine` / `Machine.fork` live fork of a running machine, the
  `machine-forked` outcome, `ForkFidelity`, and `forkFidelity(capabilities)`.
  The simulator declares `disk-fork`, accepts a capability set, and refuses to
  destroy a live-fork source before its children.
- Aligns the Machines client with the 0.1.3 release.

## 0.1.2 - 2026-09-25

- Aligns the machines contracts and client with the 0.1.2 SDK release.

## 0.1.1 - 2026-09-24

- First coordinated SDK release of machine lifecycle and operation contracts.
- Includes image qualification, checkpoints, forks, events, usage receipts,
  and provider-declared assurance levels.
