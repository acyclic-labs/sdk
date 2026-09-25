# @acyclic-labs/plugin changelog

## 0.1.5 - 2026-09-25

- Preserves executable permissions while testing a read-only installation,
  allowing the packaged launcher to complete release qualification.

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
