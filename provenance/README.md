# Provenance and disclosure gate

Every imported source group must be a reviewed snapshot from a named commit. Add
its repository, 40-character commit, original and destination paths, original
license, reviewer, and audit result to `manifest.json`. Never copy Git history,
dirty worktrees, build output, credentials, private protocol descriptors, or
operational evidence.

The repository remains private while any import is pending or while secret,
license, generated-artifact, dependency, or private-namespace scans fail.
