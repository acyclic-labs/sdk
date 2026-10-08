// Executable case ownership stays in the runner, separate from documentation vectors.
// Keep registrations as an array: constructing a Map first would erase duplicates.
/** @type {[string, string, [string, string][]][]} */
export const harnessCaseMarkers = [
  ["harness", "operation-identities-are-stable", [["rust", "executor::tests::operation_identity_rejects_changed_input"]]],
  ["harness", "native-wasm-replay-is-byte-equivalent", [["rust", "wire_codec::tests::native_event_bytes_match_cross_language_fixture"], ["typescript", "WASM event bytes match the native cross-language fixture"]]],
  ["harness", "authority-scopes-cannot-cross-aggregate-audiences", [["rust", "core::tests::mutated_or_foreign_scopes_are_rejected"]]],
  ["harness", "stream-append-uncertainty-is-queryable", [["rust", "store::tests::reconciliation_observes_a_commit_without_redispatch"]]],
  ["harness", "full-history-restores-from-checked-snapshot", [["rust", "store::tests::snapshot_reopens_with_full_stream_history"]]],
  ["harness", "fork-publication-is-atomic", [["rust", "core::tests::fork_is_invisible_until_one_seed_event_commits"]]],
  ["harness", "effect-attempts-respect-provider-guarantees", [["rust", "core::tests::at_most_once_effect_is_never_redispatched_after_uncertainty"]]],
  ["harness", "typed-approvals-bind-the-exact-action", [["rust", "interaction::tests::approval_binding_cannot_change_with_display_json"], ["rust", "runtime::tests::tool_approval_keeps_terminal_outcomes_distinct"]]],
  ["harness", "structured-parent-waits-release-capacity", [["rust", "scheduler::tests::waiting_parent_releases_execution_capacity"]]],
  ["harness", "join-preserves-child-slot-order", [["rust", "scheduler::tests::reduction_is_bound_to_the_exact_contract_and_inputs"]]],
  ["harness", "race-uses-first-authoritative-success", [["rust", "scheduler::tests::race_uses_first_observed_success_and_cancels_losers"]]],
  ["harness", "quorum-fails-when-threshold-is-unreachable", [["rust", "scheduler::tests::failed_dependencies_are_explicitly_rejectable"]]],
  ["harness", "stock-executor-replay-does-not-repeat-tools", [["rust", "executor::tests::stock_loop_replays_without_reinvoking_models_or_tools"]]],
  ["harness", "custom-executor-owns-the-whole-turn-loop", [["rust", "executor::tests::interrupted_model_stream_reconciles_without_redispatch"]]],
  ["harness", "client-replay-is-generation-fenced", [["typescript", "a replay generation cannot change without an explicit rebase"], ["typescript", "client hydrates a durable cursor before its first reconnect"], ["typescript", "rebase fences a delivery buffered by the previous replay connection"]]],
  ["harness", "client-outbox-clears-only-after-authority", [["typescript", "reconnect delivery is contiguous and clears authoritative outbox entries"], ["typescript", "IndexedDB atomically persists outbox acknowledgements and replay cursors across restart"], ["typescript", "IndexedDB preserves enqueue order across restart and isolates database namespaces"], ["typescript", "terminal admission removes a safe command from the retry outbox"]]],
  ["harness", "pagination-is-bounded-and-rebase-safe", [["typescript", "page reset fences an older in-flight response"]]],
  ["harness", "operation-control-is-protocol-scope-and-owner-bound", [["rust", "wire_api::tests::operation_control_is_protocol_scope_and_response_identity_bound"], ["typescript", "gRPC control validates echoed operation, owner, protocol, error, and retry identity"]]],
  ["harness", "recursive-cancellation-is-atomic-and-exactly-replayable", [["rust", "distributed::tests::authenticated_recursive_cancel_is_atomic_durable_and_exactly_replayable"]]],
  ["harness", "recursive-cancellation-stops-at-owner-boundaries", [["rust", "scheduler::tests::recursive_cancellation_stops_at_owner_boundaries"]]],
  ["harness", "transport-control-errors-remain-request-correlated", [["typescript", "framed control serializes observe and cancel for the same operation"], ["typescript", "correlated framed errors do not abort another operation"]]],
  ["harness", "durable-context-providers-reopen-compaction-exactly", [["rust", "context::tests::durable_sources_and_compaction_reopen_exactly"]]],
  ["harness", "coding-bundle-host-adapter-is-complete-and-executable", [["rust", "bundle::tests::coding_factory_builds_an_executable_complete_registry"]]],
  ["harness", "recursive-fork-isolation-attachments-and-project-only-merge", [["e2e", "thousand_twenty_four_recursive_forks_keep_files_private_and_merge_only_project"]]],
  ["harness", "durable-local-conversation-fork-and-merge-reopens", [["e2e", "local_reopen_preserves_ref_only_history_fork_and_parent_merge"]]],
];

// Validate the selected family before running tools or accepting case evidence.
// A scenario may assert several contracts; evidence need not be exclusive across cases.
export function registerCases(suite, family, registrations) {
  if (!Array.isArray(suite.cases)) throw new Error("suite cases must be an array");
  const names = new Set();
  for (const item of suite.cases) {
    if (typeof item?.name !== "string" || !item.name ||
        typeof item.family !== "string" || !item.family) {
      throw new Error("suite case must have a name and family");
    }
    if (names.has(item.name)) throw new Error(`duplicate suite case ${item.name}`);
    names.add(item.name);
  }
  const cases = suite.cases.filter(item => item.family === family);
  if (!cases.length) throw new Error(`suite has no ${family} cases`);
  const expected = new Set(cases.map(item => item.name));
  const markers = new Map();
  for (const [owner, name, required] of registrations) {
    if (owner !== family || !expected.has(name)) throw new Error(`unowned executable case ${name}`);
    if (markers.has(name)) throw new Error(`duplicate executable case ${name}`);
    if (!Array.isArray(required) || !required.length) throw new Error(`no executable evidence for ${name}`);
    const tests = new Set();
    for (const requirement of required) {
      if (!Array.isArray(requirement) || requirement.length !== 2 ||
          !["rust", "typescript", "e2e"].includes(requirement[0]) ||
          typeof requirement[1] !== "string" || !requirement[1]) {
        throw new Error(`invalid executable evidence for ${name}`);
      }
      const identity = JSON.stringify(requirement);
      if (tests.has(identity)) throw new Error(`duplicate evidence requirement for ${name}`);
      tests.add(identity);
    }
    markers.set(name, required);
  }
  for (const name of expected) {
    if (!markers.has(name)) throw new Error(`missing executable case ${name}`);
  }
  return { cases, markers };
}

export function requireExecutedCase(name, required, executed) {
  if (!required?.length || required.some(([runtime, test]) => !executed[runtime]?.has(test))) {
    throw new Error(`executed package evidence is missing for ${name}`);
  }
}
