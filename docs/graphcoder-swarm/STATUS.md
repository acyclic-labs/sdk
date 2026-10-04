# Local swarm implementation status

The goal remains active. All 68 entries in [requirements.json](requirements.json) remain required; the locked matrix SHA256 is `4d723c6391a234d8cf18c149960c3d45eb19459a642ff326cb8dc439dc1605ef`. Missing, skipped, failing, or flaky verification prevents completion.

## Current integration evidence

- Exact request restoration is centralized in Harness. At `6ebee3fd3`, integrated generated model-boundary contracts and registered option-policy coverage pass 20/20 native model-input regressions. See [current contract evidence](checkpoint-model-boundary-contracts-current-2026-10-04.json). The generated audit remains unqualified after a disk-full failure; a retry is running after space recovery. Other source-bound focused request, persistent-input, scope, and serialization checks are indexed in [EVIDENCE.md](EVIDENCE.md). These checks do not qualify the production recursive swarm.
- Historical rejection evidence is restored through authenticated conversation-to-journal bindings at `3febb6285`, with multi-call correlation corrected at `03797e793`. At `6d22aba49`, the archived native boundary executable overflows the default Windows test stack. The same executable with a 32 MiB test stack passes both negative cases but fails recursive child follow-up because inherited journal events belong to the parent's private volume. See [context boundary evidence](checkpoint-context-history-private-journal-2026-10-04.json). At `024e26e34`, a dedicated fixture stack runs all cases without a global stack override and preserves the same 2/3 authority failure; see [dedicated-stack evidence](checkpoint-context-dedicated-stack-2026-10-04.json). Inherited evidence must remain frozen without granting mutable parent-journal access.
- Cancellation persistence failure keeps late process success uncertain and fences redispatch after real-storage reopen. The source-bound host suite at `f97519f57` passes 25/26; its held-pipe marker failure remains recorded in [cancellation evidence](checkpoint-cancellation-persistence-fence-2026-10-04.json). At `d00d687de`, the repaired fixture and complete host suite pass 26/26 against the archived Windows executable. See [current host evidence](checkpoint-host-execution-current-2026-10-04.json). Installed native cleanup and the complete swarm fault matrix remain unqualified.
- Public receipt reconciliation rejects substituted operation and effect identities at `5e78977b4`, passing its focused case. See [receipt identity evidence](checkpoint-reconcile-receipt-identities-2026-10-04.json).
- Git transition routing at `275fe2da5` passes its public abort regression. See [Git transition evidence](checkpoint-git-transition-routing-2026-10-04.json). This does not qualify approved root writeback.
- The most recent source-bound production coordinator run, at `ee7cb5f40`, passes 0/1 recursive model swarm cases and 1/5 recovery fault cases. See [production evidence](checkpoint-current-production-swarm-2026-10-04.json). Later context changes require a fresh production run after their focused failures are repaired.

At source 7d951663e, the native recursive boundary suite passes 3/3, including exact prefix checks on every captured child request and an explicit rejection follow-up. The executable and log are archived in checkpoint-native-recursive-prefix-pass-2026-10-04.json. This closes the focused private-prefix fixture failures; production coordinator capture/publication and recovery qualification remain open.

## Required integration and qualification gates

1. Preserve exact recursive prefixes and rejection evidence across child follow-up, parallel batches, deeper forks, and restart. Preserve the dedicated native fixture stack and resolve the production capture/publication revision mismatch.
2. Integrate and independently qualify current-root durable resource budgeting, communications/waits, and lazy activation/listing. Isolated worker checkpoints are candidate changes, not final qualification.
3. Complete effect recovery and process ownership, including output overflow, responsive cancellation during blocked stdin, descendant cleanup, retained uncertain outcomes, and installed native transport.
4. Provide a usable public inspect/approve/apply root-writeback path. Bind every physical root mutation to exact durable approval and reconcile concurrent user edits, including deletions and crash replay.
5. Build fresh distributables after the final source change. Run the entire locked matrix through those artifacts, including Windows terminal PTY, package consumption, generated bindings, provider conformance, Filesystem/plugin regressions, and applicable platform lanes.
6. Audit library ownership and dependency boundaries, record final source/suite/descriptor/artifact digests, and confirm committed isolated branch state with no merge.

Historical checkpoint results and failures are retained in [EVIDENCE.md](EVIDENCE.md) and their source-bound receipts. They must not be reused to qualify newer source or broader requirements.

## Boundaries

Harness owns orchestration, model inputs, forks, communication, admission, effects, and recovery. Filesystem owns workspace semantics and its host execution adapter. GraphCoder remains a composition and terminal wrapper.

Models are mocked. No sandbox or cloud is implemented. Workspace routing is not process confinement. Host commands require exact approval and exclude inherited credentials. Root writeback requires approval and concurrent-edit reconciliation. The original checkout must remain untouched; all implementation stays on the isolated `codex/graphcoder-sdk` branch without merging.



