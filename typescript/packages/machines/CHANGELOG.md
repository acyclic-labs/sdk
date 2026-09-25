# @acyclic-labs/machines changelog

## Unreleased

- Add `forkMachine` / `Machine.fork` live fork of a running machine, the
  `machine-forked` outcome, `ForkFidelity`, and `forkFidelity(capabilities)`.
  The simulator declares `disk-fork`, accepts a capability set, and refuses to
  destroy a live-fork source before its children.

## 0.1.2 - 2026-09-25

- Aligns the machines contracts and client with the 0.1.2 SDK release.

## 0.1.1 - 2026-09-24

- First coordinated SDK release of machine lifecycle and operation contracts.
- Includes image qualification, checkpoints, forks, events, usage receipts,
  and provider-declared assurance levels.
