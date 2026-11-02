---
version: 1
tools:
  - capability: shell
    variant: shell
    revision: codex-array-0.20.0-patch-67f0994b-v1
    arguments: {command: $command, cwd: $workdir, timeout_ms: $timeout}
  - capability: edit
    variant: apply_patch
    revision: codex-array-0.20.0-patch-67f0994b-v1
    implementation: apply_patch
    arguments: {patch: $input}
---
