---
name: verify
description: Verify a change with the smallest check that exercises its intended behavior.
---
Read the change and identify the behavior that could fail. Run the covering check or build, using the repository's ordinary commands. A failure is evidence to diagnose, not a reason to raise a timeout or retry unchanged. Check the resulting diff for accidental files and unrelated changes. Before reporting completion, state what changed, the exact check and result, and any remaining uncertainty. Never invent a passing result.
