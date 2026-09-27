# @acyclic-labs/plugin changelog

## Unreleased

- Merging a subagent carries everything it did to its parent: edits,
  creations, deletions and renames of files it only read from the shared
  checkout, and extended attributes on Linux. A deletion survives
  `acyclic git merge --continue` and crash recovery, and a parent's newer edit
  of the same file is kept.
- Sibling subagents no longer conflict on files they never touched. Files and
  directories created independently under one name merge by path; only
  differing contents conflict.
- Forks keep working after the parent's checkout changes underneath them
  (no more stale-handle errors creating files), and reading a file through a
  fork no longer rewrites anything in the parent.
- Aborting a conflict that changed nothing in the parent succeeds.
- Aborting a conflicted merge restores the parent exactly: it no longer
  deletes files the subagent only read, and it succeeds when the subagent
  added files to an existing directory.
- A merge carries only what the subagent wrote; files it only read are no
  longer copied into the parent's history.
- Editing a file in the root checkout no longer hides the rest of its
  directory from subagents started afterwards.
- A deletion merged from a grandchild shows in its parent's workspace at once.
- On Windows, subagents can rename directories they inherited from the
  parent's checkout (it failed with "The request is not supported").
- On macOS, listing a directory no longer fails with "Stale NFS file
  handle" when the parent's checkout changes while the listing starts (it
  did on first use of a fresh mount about one run in ten).
- Windows runs the same fork/join conformance suite as Linux and macOS.
- A crashed installer's lock is reclaimed at once even when the OS has given
  its process ID to another process.
- The Codex hook server keeps answering after a request too large to isolate
  or a failing hook call, which it answers as a denial for isolated tools
  instead of exiting.

## 0.1.5 - 2026-09-25

- Preserves executable permissions while testing a read-only installation,
  allowing the packaged launcher to complete release qualification.
- Recognizes the expanded native-mount certification coverage and continues to
  require every baseline case when installing the universal package.

## 0.1.4 - 2026-09-25

- Restores executable modes for every Linux and macOS release binary and
  validates those modes in the universal package.

## 0.1.3 - 2026-09-25

- Rebuilds the universal `acyclic` distribution from the qualified SDK source,
  including the target-architecture release receipt fix.

## 0.1.2 - 2026-09-25

- Consolidates the coding-agent distribution on the `acyclic` binary and aligns its package metadata with SDK 0.1.2.

## 0.1.1 - 2026-09-24

- Aligns the universal coding-agent plugin and `acyclic` binary with SDK 0.1.1.
- Codex and Claude Code integrations share one qualified cross-platform
  package, with native mount qualification and host-specific capability checks.
