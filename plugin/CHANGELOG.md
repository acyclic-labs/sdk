# @acyclic-labs/plugin changelog

## 0.2.0 - Unreleased

- Align the unified SDK candidate with the breaking Objects v2 public package transition.

## Unreleased

- On Linux, a file opened for reading keeps working after another handle
  writes to it and syncs: it reads the new contents instead of failing with
  "Stale file handle".
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
- On a Windows Dev Drive (ReFS), which drops some change notifications,
  edits and deletions in the root checkout still reach subagents started
  afterwards: every refresh checks recorded files against the disk.
- On Windows, subagents start faster: a subagent's checkout is written by
  several threads at once.
- On Windows, subagents see symbolic links from the parent's checkout,
  also on a Dev Drive (which needs Developer Mode there).
- On Windows, a file renamed before anything reads it can be read at once
  under its new name; it was briefly missing about one time in thirty.
- On Windows, antivirus scans no longer break subagents: a file deleted
  while a scanner holds it open no longer fails listings with "Access is
  denied", and staged copies no longer linger after a scan.
- On Windows, a file's modified time set through a handle open for writing
  now sticks. It reverted for up to one edit in five when another process
  had the file open, and for many empty files even when none did. A
  subagent's checkout is now written as ordinary files, so starting a
  subagent over 2,000 small files takes about 0.6 s instead of 0.2–0.3 s.
  A file the parent's checkout changes is replaced in one step: readers see
  the old contents or the new, never an empty or partly written file.
- On Windows, a subagent started while the parent's checkout is changing
  sees its whole checkout as soon as it starts; a directory could be
  missing for a moment.
- On Windows, a directory the parent's checkout puts in place of a link a
  subagent's program holds open appears once the program closes it; it
  could be lost.
- On Windows, ending a subagent no longer fails with "Access is denied"
  when a scanner, or a program it just ran, still holds a file for a
  moment.
- On a Windows Dev Drive (ReFS), capturing a sparse file right after it is
  written stores only its data; it could store the whole file.
- On Windows, saving a file in the root checkout while a subagent reads it
  no longer fails with "Access is denied": editors and `git` save by
  replacing the file, which Windows refused while any reader held it. The
  subagent's read now gives way and reads the saved file instead.
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
