#!/usr/bin/env node
// Qualify a production oracle and its retained mutation.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { closeSync, mkdirSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
const nativeHarness = ["--features", "test-support"];
const bindingFeatures = ["--features", "test-support"];
const bindingNextest = ["--profile", "ci", "--retries", "0", "--no-tests=fail", "--no-fail-fast", "--failure-output", "final"];
const cleanupTests = [
  "local::workspace::volumes::publication_tests::cancelled_command_retires_before_next_entrant_and_preserves_acknowledged_head",
  "local::tests::agents::a_shell_call_stages_its_output_on_the_private_volume",
  "local::owner::publication_tests::private_legacy_owner_migrates_once_without_changing_its_original_identity",
  "local::owner::publication_tests::concurrent_owner_migration_cannot_publish_another_callers_provisional_key",
  "local::owner::publication_tests::explicit_file_custody_reopens_the_original_owner_without_native_fallback",
];
const claims = {
  "filesystem-lineage-canonical": [
    "rust/crates/filesystem/src/workspace/identity.rs", "acyclic-fs",
    "lineage::tests::persisted_lineage_names_require_exact_canonical_bytes",
    "noncanonical lineage registration was accepted", ["--features", "test-support"],
    { oracle: "rust/crates/filesystem/src/lineage/tests.rs",
      mutations: [{ patch: "rust/crates/filesystem/fuzz/mutations/lineage_noncanonical_name.patch",
        label: "noncanonical-lineage-name", diagnostic: "noncanonical lineage registration was accepted" }] },
  ],
  "actors-live-update-canonical": [
    "rust/crates/actors/src/live_update.rs", "acyclic-codec-fuzz", "",
    "live-update document admission differs from canonical writer", [],
    { oracle: "rust/codec-fuzz/src/lib.rs",
      command: ["run", "--locked", "-p", "acyclic-codec-fuzz", "--example", "replay", "--", "actor_codecs"],
      invoked: "Running ", negativeExit: 1, failed: "corpus replay failed:",
      completed: ["replayed ", " captured inputs"],
      mutations: [{ patch: "rust/proofs/actors-live-update-canonical/mutation.patch",
        priorOraclePatch: "rust/proofs/actors-live-update-canonical/prior-oracle.patch",
        label: "noncanonical-admission", diagnostic: "live-update document admission differs from canonical writer" }] },
  ],
  "settings-read-producer": [
    "rust/crates/harness-core/src/config/evaluate/composition.rs", "acyclic-harness-core",
    "config::evaluate::model::tests::generated_settings_match_fresh_evaluation",
    "plugin composition cached an operational read failure", ["--features", "test-support"],
    { oracle: "rust/crates/harness-core/src/config/evaluate/model/plugins.rs",
      nextest: ["--profile", "ci", "--no-fail-fast", "--retries", "0", "--no-tests=fail"],
      mutations: [
        { patch: "rust/proofs/settings-read-producer/composition.patch", label: "plain-composition-read-became-semantic", diagnostic: "plugin composition cached an operational read failure" },
        { patch: "rust/proofs/settings-read-producer/plugin.patch", source: "rust/crates/harness-core/src/plugins.rs", label: "plain-plugin-read-became-semantic", diagnostic: "plugin decision cached an operational read failure" },
      ] },
  ],
  "settings-admission-producer": [
    "rust/crates/harness/src/local/agent_settings.rs", "acyclic-harness",
    "local::agent_settings::tests::imported_settings_keep_the_original_directory_read_refusal",
    "settings admission lost its original read producer", ["--features", "test-support"],
    { nextest: ["--profile", "ci", "--no-fail-fast", "--retries", "0", "--no-tests=fail"] },
  ],
  "plugin-input-cut": [
    "rust/crates/harness-core/src/plugins.rs", "acyclic-harness-core",
    "config::evaluate::model::tests::generated_settings_match_fresh_evaluation",
    "plugin decision lost an original manifest or hook observation", ["--features", "test-support"],
    { oracle: "rust/crates/harness-core/src/config/evaluate/model/plugins.rs",
      nextest: ["--profile", "ci", "--no-fail-fast", "--failure-output", "immediate-final", "--retries", "0", "--no-tests=fail"],
      mutations: [
        { patch: "rust/proofs/plugin-input-cut/identity.patch", label: "discarded-original-hook-bytes", diagnostic: "plugin decision lost an original manifest or hook observation" },
        { patch: "rust/proofs/plugin-input-cut/operation.patch", label: "operational-plugin-read-became-semantic", diagnostic: "plugin decision cached an operational read failure" },
        { patch: "rust/proofs/plugin-input-cut/read.patch", label: "repeated-original-hook-read", diagnostic: "plugin decision repeated an original file read" },
      ] },
  ],
  "native-unlink-completion-credit": [
    "rust/crates/filesystem/src/native_mount/fuse/lookup.rs", "acyclic-fs",
    "native_mount::fuse::tests::detach::unlink_completion_retains_copy_credit_after_source_acceptance",
    "unlink refused after its source was accepted",
    ["--no-default-features", ],
    { mutations: [
      { patch: "rust/proofs/native-unlink-completion-credit/late-preparation.patch", label: "late-preparation", diagnostic: "unlink refused after its source was accepted" },
      { source: "rust/crates/filesystem/src/demand/reservations.rs", patch: "rust/proofs/native-unlink-completion-credit/early-refund.patch", label: "early-refund", diagnostic: "completion corrupted its original ledger" },
    ], positive: [
      "native_mount::fuse::tests::detach::unlink_completion_retains_credit_after_source_acceptance",
      "native_mount::fuse::tests::detach::unlink_completion_retains_copy_credit_after_source_acceptance",
      "native_mount::fuse::tests::detach::rename_completion_retains_credit_after_source_acceptance",
      "native_mount::fuse::tests::detach::allocation_refusal_precedes_unlink_and_replacement",
    ] },
  ],
  "native-rename-reviewed-completion": [
    "rust/crates/filesystem/src/native_mount/fuse/identity.rs", "acyclic-fs",
    "native_mount::fuse::tests::cache::rename_completion_preserves_only_current_bindings",
    "rename certified a replaced descendant",
    ["--no-default-features", ],
    { mutations: [
      { patch: "rust/proofs/native-rename-reviewed-completion/stale-identity.patch", label: "stale-identity", diagnostic: "rename certified a replaced descendant" },
      { source: "rust/crates/filesystem/src/native_mount/adapter/installation.rs", patch: "rust/proofs/native-rename-reviewed-completion/non-directory.patch", label: "non-directory", diagnostic: "path traversal encountered a non-directory" },
    ] },
  ],
  "native-rename-completion-credit": [
    "rust/crates/filesystem/src/native_mount/fuse/lookup.rs", "acyclic-fs",
    "native_mount::fuse::tests::detach::rename_completion_retains_credit_after_source_acceptance",
    "rename refused after its source was accepted",
    ["--no-default-features", ],
    { mutations: [
      { patch: "rust/proofs/native-rename-reviewed-completion/completion-credit.patch", label: "completion-credit", diagnostic: "rename refused after its source was accepted" },
    ], positive: [
      "native_mount::fuse::tests::detach::rename_completion_retains_credit_after_source_acceptance",
      "native_mount::fuse::tests::detach::allocation_refusal_precedes_unlink_and_replacement",
    ] },
  ],
  "native-rename-binding-completion": [
    "rust/crates/filesystem/src/native_mount/fuse/identity.rs", "acyclic-fs",
    "native_mount::fuse::tests::cache::node_changes_preserve_cached_name_bindings",
    "a node change must not detach its unchanged name: RenamedFile",
    ["--no-default-features", ],
    { positive: [
      "native_mount::fuse::tests::cache::node_changes_preserve_cached_name_bindings",
      "native_mount::fuse::tests::detach::prepared_rename_cannot_resurrect_a_concurrent_forget",
      "native_mount::fuse::tests::detach::forgotten_inode_negative_control",
    ] },
  ],
  "metadata-closure-work": [
    "rust/crates/filesystem/src/kernel/closure/walk.rs", "acyclic-fs",
    "facade::tests::detached::metadata_reads_preserve_the_original_decoder_work",
    "metadata closure discarded its original decoder work",
    ["--no-default-features", ],
  ],
  "metadata-reader-work": [
    "rust/crates/filesystem/src/kernel/metadata.rs", "acyclic-fs",
    "facade::tests::detached::metadata_reads_preserve_the_original_decoder_work",
    "metadata decoder bypassed its original copy admission",
    ["--no-default-features", ],
  ],
  "native-error-custody": [
    "rust/crates/stream/src/local/local_inner.rs", "acyclic-stream",
    "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers",
    "failure watch replaced the original native I/O error", ["--no-default-features", "--features", "test-support"],
    { oracle: "rust/crates/stream/src/local/tests/failure.rs", mutations: [
      { patch: "rust/proofs/native-error-custody/watch.patch", label: "watch", diagnostic: "failure watch replaced the original native I/O error" },
      { source: "rust/crates/stream/src/native/work.rs", patch: "rust/proofs/native-error-custody/delivered.patch", label: "delivered", diagnostic: "failure watch replaced the original native I/O error" },
      { source: "rust/crates/stream/src/local/native_mutations.rs", patch: "rust/proofs/native-error-custody/guarded.patch", label: "guarded", diagnostic: "failure watch replaced the original native I/O error" },
      { source: "rust/crates/stream/src/local/prepared_commit.rs", patch: "rust/proofs/native-error-custody/prepared.patch", label: "prepared", diagnostic: "failure watch replaced the original native I/O error" },
    ] },
  ],
  "policy-time-interval": [
    "rust/crates/filesystem/src/access/trie.rs", "acyclic-fs",
    "access::review::policy_fence_and_native_custody_preserve_pinned_views",
    "saved view must reject a revived deny or expired capability", ["--no-default-features", "--features", "test-support"],
    { target: ["--test", "main"], oracle: "rust/crates/filesystem/tests/access_review.rs", mutations: [
      { patch: "rust/proofs/policy-time-interval/view.patch", label: "revived-deny-view", diagnostic: "saved view must reject a revived deny or expired capability" },
      { source: "rust/crates/filesystem/src/access/policy_fence.rs", patch: "rust/proofs/policy-time-interval/fence.patch", label: "revived-deny-fence", diagnostic: "publication fence must reject a revived deny or expired capability" },
    ] },
  ],
  "exact-extraction-custody": [
    "rust/crates/collections/src/lib.rs", "acyclic-collections",
    "tests::prefix::prefix_extraction_preserves_exact_keys_original_custody_and_bounded_work",
    "exact extraction retired original backing", [],
    { oracle: "rust/crates/collections/src/tests/prefix.rs", mutations: [
      { patch: "rust/crates/collections/src/tests/early_exact_retirement.patch", label: "early-retirement", diagnostic: "exact extraction retired original backing" },
    ] },
  ],
  "recovered-metadata-custody": [
    "rust/crates/filesystem/src/kernel/serde_decode/original.rs", "acyclic-fs",
    "core_state::codec_fuzz::production_metadata_corpus_preserves_original_decode_custody",
    "metadata decoder replaced original accounting ledger", ["--no-default-features", "--features", "test-support"],
    { oracle: "rust/crates/filesystem/src/core_state/codec_fuzz.rs", mutations: [
      { patch: "rust/crates/filesystem/fuzz/mutations/metadata_decode_detached_frame.patch", label: "detached-decoder", diagnostic: "metadata decoder replaced original accounting ledger" },
      { source: "rust/crates/filesystem/src/kernel/serde_decode.rs", patch: "rust/crates/filesystem/fuzz/mutations/metadata_decode_unfunded_bytes.patch", label: "unfunded-bytes", diagnostic: "metadata decoder omitted owned byte payload funding" },
    ] },
  ],
  "recovered-record-retirement": [
    "rust/crates/filesystem/src/record_store.rs", "acyclic-fs",
    "record_store::tests::record_retirement_never_holds_the_record_lock",
    "record backing retired while its record lock was held", ["--no-default-features", "--features", "test-support"],
    { mutations: [{ patch: "rust/crates/filesystem/fuzz/mutations/record_retirement_lock.patch", label: "locked-retirement", diagnostic: "record backing retired while its record lock was held" }] },
  ],

  "query-navigation": [
    "rust/crates/harness-tui/src/query_controller.rs", "acyclic-harness-tui",
    "query_page::tests::wrapped_query_pages_reveal_every_row_before_the_source_continuation",
    "pending refresh must leave every admitted row reachable", [],
    { mutations: [
      { patch: "rust/proofs/query-navigation/progress.patch", label: "progress", diagnostic: "pending refresh must leave every admitted row reachable" },
      { patch: "rust/proofs/query-navigation/safety.patch", label: "safety", diagnostic: "pending navigation must preserve the admitted page" },
      { patch: "rust/proofs/query-navigation/cost.patch", label: "cost", diagnostic: "local query scroll must not allocate" },
    ] },
  ],
  "objects-explicit-close": [
    "rust/crates/objects/src/local.rs", "acyclic-objects",
    "local::outcome_tests::owned_close_reports_physical_completion_and_failure",
    "implicit Drop performed unobserved durability", ["--no-default-features", "--features", "test-support"],
    { mutations: [{ patch: "rust/codec-fuzz/mutations/objects-implicit-close.patch", label: "implicit-close", diagnostic: "implicit Drop performed unobserved durability" }] },
  ],
  "objects-cost-observation": [
    "rust/crates/objects/src/tests.rs", "acyclic-objects",
    "tests::a_repeat_put_observes_metadata_and_probes_presence_without_writes",
    "version observations must remain distinct from payload reads", ["--no-default-features", "--features", "test-support"],
    { mutations: [{ patch: "rust/codec-fuzz/mutations/objects-conflated-observations.patch", label: "conflated-observations", diagnostic: "version observations must remain distinct from payload reads" }] },
  ],
  "objects-cost-forwarding": [
    "rust/crates/objects/src/tests.rs", "acyclic-objects",
    "tests::a_local_put_syncs_nothing_and_its_commit_syncs_the_log_once",
    "cost recorder suppressed the original local flush", ["--no-default-features", "--features", "test-support"],
    { mutations: [{ patch: "rust/codec-fuzz/mutations/objects-recorder-skipped-flush.patch", label: "skipped-flush", diagnostic: "cost recorder suppressed the original local flush" }] },
  ],
  "fixed-index-read-admission": [
    "rust/crates/collections/src/lib.rs", "acyclic-collections",
    "tests::ordered::ordered_seek_and_mutation_preserve_original_credit_and_sparse_cursors",
    "cancelled empty exact read must be refused", [],
    {
      oracle: "rust/crates/collections/src/tests/ordered.rs",
      nextest: ["--no-tests=fail"],
      mutations: [
        { patch: "rust/crates/collections/src/tests/unchecked_empty_read.patch", label: "unchecked-empty-read", diagnostic: "cancelled empty exact read must be refused" },
        { patch: "rust/crates/collections/src/tests/late_remove_admission.patch", label: "late-remove-admission", diagnostic: "refused removal must preserve original owner" },
        { source: "rust/crates/collections/src/recent.rs", patch: "rust/crates/collections/src/tests/swallowed_window_refusal.patch", label: "swallowed-window-refusal", diagnostic: "recent window refusal must not become a cache miss" },
      ],
    },
  ],
  "persistence-cut-boundary": [
    "rust/crates/stream/src/local/persistence_cut.rs", "acyclic-stream",
    "local::tests::journal_batch::cuts::a_batch_cut_recovers_only_its_flushed_prefix",
    "a frame written before the cut is not flushed", ["--no-default-features", "--features", "test-support"],
    { oracle: "rust/crates/stream/src/local/tests/journal_batch.rs",
      nextest: ["--profile", "ci", "--no-tests=fail", "--retries", "0"],
      mutations: [
        { patch: "rust/proofs/persistence-cut-boundary/flush.patch", label: "unflushed-prefix", diagnostic: "a frame written before the cut is not flushed" },
        { patch: "rust/proofs/persistence-cut-boundary/fence.patch", label: "unfenced-installation", diagnostic: "cut escaped the original mutation fence" },
        { patch: "rust/proofs/persistence-cut-boundary/failure.patch", label: "lost-owner", diagnostic: "cut retained a lost original owner" },
      ] },
  ],
  "stream-codec-path-admission": [
    "rust/crates/stream/src/codec_fuzz.rs", "acyclic-stream",
    "codec_fuzz::emitted_persistence_seeds_satisfy_shared_oracle",
    "shared persistence oracle rejected", ["--no-default-features", "--features", "test-support"],
    { mutations: [
      { patch: "rust/proofs/stream-codec-path-admission/followed.patch", label: "refused-follow-context", diagnostic: "shared persistence oracle rejected" },
      { patch: "rust/proofs/stream-codec-path-admission/foreign.patch", label: "refused-foreign-context", diagnostic: "shared persistence oracle rejected" },
    ] },
  ],
  "revision-floor-merge": [
    "rust/crates/stream/src/lib/revision.rs", "acyclic-stream",
    "revision::tests::minimum_cut_refuses_incarnation_replacement_in_every_input_order",
    "revision floor lost an original participant or maximum", ["--no-default-features"],
    { mutations: [
      { patch: "rust/proofs/revision-floor-merge/participant.patch", label: "discarded-explicit-participant", diagnostic: "revision floor lost an original participant or maximum" },
      { patch: "rust/proofs/revision-floor-merge/maximum.patch", label: "regressed-explicit-maximum", diagnostic: "revision floor lost an original participant or maximum" },
      { patch: "rust/proofs/revision-floor-merge/incarnation.patch", label: "admitted-foreign-incarnation", diagnostic: "revision floor accepted a foreign incarnation" },
    ] },
  ],
  "merge-json-refusal": [
    "rust/crates/filesystem/src/kernel/allocation/writer.rs", "acyclic-fs",
    "workspace::tests::merge_drivers::merge_directory_json_admission_precedes_output_allocation",
    "refused JSON allocated private serializer backing", ["--no-default-features", ],
  ],
  "merge-json-admission": [
    "rust/crates/filesystem/src/workspace/join_directory.rs", "acyclic-fs",
    "workspace::tests::merge_drivers::merge_directory_json_admission_precedes_output_allocation",
    "serializer allocated before original admission", ["--no-default-features", ],
  ],
  "merge-text-admission": [
    "rust/crates/filesystem/src/kernel/allocation/writer.rs", "acyclic-fs",
    "workspace::tests::merge_drivers::merge_directory_pages_preserve_pins_and_non_text_conflicts",
    "text allocated before original admission", ["--no-default-features", ],
  ],
  "allocation-first-event": [
    "rust/crates/test-support/src/lib.rs", "acyclic-test-support",
    "tests::accounting_matches_complete_allocation_lifetimes",
    "first allocation event differs from physical trace", [],
  ],
  "native-line-eof": [
    "rust/crates/native-runtime/src/lines.rs", "acyclic-native-runtime",
    "lines::tests::incremental_eof_matches_async_lines_at_every_split",
    "incremental EOF lost the original final frame", ["--features", "test-support"],
    { additionalTests: ["lines::tests::oversize_and_invalid_frames_cannot_hide_the_next_complete_line"],
      additionalDiagnostics: ["EOF discarded the recovered final frame"] },
  ],
  "terminal-drain-stop": [
    "rust/crates/harness-tui/tests/terminal_frames.rs", "acyclic-harness-tui",
    "terminal_drain_cancellation_does_not_require_slave_closure",
    "a retained PTY slave must not keep a cancelled drainer waiting", ["--features", "tui"],
    { target: ["--test", "terminal_frames"] },
  ],
  "native-usage-counters": [
    "rust/crates/harness-core/src/executor/model_usage.rs", "acyclic-harness-core",
    "executor::model_usage::tests::usage_dialects_preserve_units_absence_and_malformed_controls",
    "Test failed:", ["--features", "filesystem-local,native-execution,test-support"],
  ],
  "native-event-cut": [
    "rust/crates/harness/src/local/host_process/children.rs", "acyclic-harness",
    "local::host_process::children::tests::an_exited_child_requeues_captured_output_without_waiting_for_stdout_eof",
    "exited native terminal without LF was discarded", nativeHarness,
  ],
  "native-event-retention": [
    "rust/crates/harness/src/local/host_process.rs", "acyclic-harness",
    "local::host_owner::tests::native_event_log_keeps_late_usage_and_resume_after_recovery",
    "native event publication lost records after the old cutoff", nativeHarness,
  ],
  "capture-marker-retirement": [
    "rust/crates/harness-core/src/filesystem/native_process/capture/tests.rs", "acyclic-harness-core",
    "filesystem::native_process::capture::tests::interactive_completion_retains_input_producer_and_cleanup",
    "retired capture retained its unused marker producer", ["--features", "filesystem-local,native-execution,test-support"],
  ],
  "append-invocation": [
    "rust/crates/harness-core/src/filesystem/execution_journal/summary/turn_cost.rs", "acyclic-harness-core",
    "filesystem::execution_journal::summary::tests::turn_cost::append_work_is_bounded_per_invocation_across_history",
    "append inherited prior invocation work", ["--features", "filesystem-local,native-execution,test-support"],
  ],
  "effective-read-selection": [
    "rust/crates/harness-core/src/tool/files/read.rs", "acyclic-harness-core",
    "filesystem::memory::tests::layered_reads::every_effective_projection_keeps_the_original_probe_generation",
    "projection reselected head after its original home probe", ["--features", "filesystem"],
    {
      oracle: "rust/crates/harness-core/src/filesystem/memory/tests/layered_reads.rs",
      mutations: [
        { source: "rust/crates/harness-core/src/tool/files/executor.rs", patch: "rust/proofs/effective-read-selection/preflight.patch", label: "physical-preflight-before-layer-selection", diagnostic: "projection reselected head after its original home probe" },
        { patch: "rust/proofs/effective-read-selection/selection.patch", label: "discarded-original-layer-selection", diagnostic: "projection reselected head after its original home probe" },
        { source: "rust/crates/harness-core/src/tool/files/authorization.rs", patch: "rust/proofs/effective-read-selection/token.patch", label: "bypassed-original-content-token-grant", diagnostic: "virtual path lost the original content token grant",
          command: ["test", "--locked", "-p", "acyclic-harness-core", "--features", "filesystem", "--lib", "filesystem::memory::tests::layered_reads::effective_reads_preserve_the_selected_layers_authority", "--", "--exact", "--nocapture"] },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-harness-core", "--features", "filesystem", "--lib", "--", "--exact", "--nocapture",
        "filesystem::memory::tests::layered_reads::every_effective_projection_keeps_the_original_probe_generation",
        "filesystem::memory::tests::layered_reads::explicit_versions_keep_their_selection_without_a_new_home_probe",
        "filesystem::memory::tests::layered_reads::seed_fallback_and_off_markers_keep_the_original_layer_decision",
        "filesystem::memory::tests::layered_reads::effective_reads_preserve_the_selected_layers_authority",
        "filesystem::memory::tests::projections::cached_line_reads_keep_original_receipts_outside_the_source_volume"],
      positiveCount: 5,
      completed: ["5 passed; 0 failed"],
    },
  ],
  "task-binding-selection": [
    "rust/crates/harness/src/local/workspace/bindings.rs", "acyclic-harness",
    "local::workspace::bindings::tests::original_preparation_has_finite_generation_and_credit",
    "lost original producer did not finish its observer", bindingFeatures,
    { nextest: bindingNextest, mutations: [
      { patch: "fuzz/mutations/task_binding_lost_producer.patch", label: "producer-loss", diagnostic: "lost original producer did not finish its observer" },
      { patch: "fuzz/mutations/task_binding_late_retirement.patch", label: "late-retirement", diagnostic: "late retirement removed the replacement owner" },
      { patch: "fuzz/mutations/task_binding_retained_credit.patch", label: "retained-credit", diagnostic: "retired binding generations retained original allocation credit" },
      { patch: "fuzz/mutations/task_binding_lookup_cost.patch", label: "lookup-cost", diagnostic: "binding lookup grew beyond the fixed-width path" },
    ] },
  ],
  "task-binding-observer": [
    "rust/crates/harness/src/local/workspace/bindings.rs", "acyclic-harness",
    "local::workspace::task_views::tests::binding_preparation_releases_the_checkout_owner",
    "registered binding observer lost its successful preparation", bindingFeatures,
    { nextest: bindingNextest, oracle: "rust/crates/harness/src/local/workspace/task_views/tests.rs", mutations: [
      { patch: "fuzz/mutations/task_binding_observer_result.patch", label: "result-loss", diagnostic: "registered binding observer lost its successful preparation" },
    ] },
  ],
  "task-binding-checkout": [
    "rust/crates/harness/src/local/workspace/task_views.rs", "acyclic-harness",
    "local::workspace::task_views::tests::binding_preparation_releases_the_checkout_owner",
    "binding preparation retains the unrelated checkout owner", bindingFeatures,
    { nextest: bindingNextest, oracle: "rust/crates/harness/src/local/workspace/task_views/tests.rs", mutations: [
      { patch: "fuzz/mutations/task_binding_checkout_owner.patch", label: "checkout-owner", diagnostic: "binding preparation retains the unrelated checkout owner" },
    ] },
  ],
  "task-binding-physical": [
    "rust/crates/harness/src/local/workspace/projection.rs", "acyclic-harness",
    "local::workspace::task_views::tests::native_guard_retains_the_original_binding_until_physical_retirement",
    "physical native guard lost the original binding owner", bindingFeatures,
    { nextest: bindingNextest, oracle: "rust/crates/harness/src/local/workspace/task_views/tests.rs", mutations: [
      { patch: "fuzz/mutations/task_binding_physical_owner.patch", label: "physical-owner", diagnostic: "physical native guard lost the original binding owner" },
    ] },
  ],
  "inbox-settlement": [
    "rust/crates/harness-core/src/agent/records.rs", "acyclic-harness-core",
    "agent::checkpoint::tests::bounded_record_fields_and_settled_checkpoints_fit_their_frames",
    "a conversation entry fits", ["--features", "filesystem-local,test-support"],
  ],
  "cli-failure-output": [
    "rust/crates/harness/src/completion.rs", "acyclic-plugin",
    "runner::lifecycle::tests::failure_output_retains_cause_and_progress_with_native_backpressure",
    "failure output retained a lost shutdown producer", [],
    { mutations: [
      { patch: "rust/proofs/cli-failure-output/shutdown.patch", label: "shutdown-omitted", diagnostic: "failure output retained a lost shutdown producer" },
    ] },
  ],
  "native-hard-link-detached-facts": [
    "rust/crates/filesystem/src/native_mount/lazy/mutations.rs", "acyclic-fs",
    "native_mount::lazy::tests::publication::source_hard_link_open_unlink_write_preserves_surviving_alias",
    "hard-link reply discarded current detached file facts",
    ["--no-default-features", ],
  ],
  "call-dependent-resource": [
    "rust/crates/stream/src/session/resources.rs", "acyclic-stream",
    "session::tests::calls::dependent_resources_keep_original_call_grants_and_live_trust",
    "dependent scope escaped its original declared boundary", ["--features", "test-support"],
    {
      mutations: [
        { patch: "rust/proofs/call-dependent-resource/scope.patch", label: "discarded-dependent-resource-boundary", diagnostic: "dependent scope escaped its original declared boundary" },
        { patch: "rust/proofs/call-dependent-resource/credential.patch", label: "detached-original-current-credential", diagnostic: "dependent resource detached from the original credential renewal" },
        { patch: "rust/proofs/call-dependent-resource/policy.patch", label: "discarded-dependent-owner-policy", diagnostic: "dependent disclosure ignored the owner's current policy" },
        { patch: "rust/proofs/call-dependent-resource/index.patch", label: "discarded-dependent-index-normalization", diagnostic: "resource index differs from its original authorized scope" },
      ],
    },
  ],
  "config-composition-cut": [
    "rust/crates/harness-core/src/config/evaluate/composition.rs", "acyclic-harness-core",
    "config::evaluate::model::tests::generated_settings_match_fresh_evaluation",
    "composition discarded its original refused module cut", [],
    {
      mutations: [
        { patch: "rust/proofs/config-composition-cut/refusal.patch", label: "discarded-original-refused-module-cut", diagnostic: "composition discarded its original refused module cut" },
        { patch: "rust/proofs/config-composition-cut/generations.patch", label: "discarded-original-layer-generations", diagnostic: "composition identity discarded its original layer generations" },
      ],
    },
  ],
  "config-retained-cut": [
    "rust/crates/harness-core/src/config/evaluate/prepared.rs", "acyclic-harness-core",
    "config::evaluate::model::tests::generated_settings_match_fresh_evaluation",
    "original dependency observation was discarded", [],
    {
      mutations: [
        { patch: "rust/proofs/config-retained-cut/absence.patch", label: "discarded-original-absent-import", diagnostic: "original dependency observation was discarded" },
        { patch: "rust/proofs/config-retained-cut/identity.patch", label: "discarded-original-read-cut-identity", diagnostic: "settings input identity aliased a different dependency cut" },
      ],
    },
  ],
  "trust-fenced-close-progress": [
    "rust/crates/stream/src/trust_feed.rs", "acyclic-stream",
    "trust_anchor::closing_a_fenced_environment_ends_its_original_anchor_wait",
    "closed follower left its original anchor wait parked", ["--features", "test-support"],
    { target: ["--test", "main"], patchPath: "rust/proofs/trust-fenced-close-progress/mutation.patch", negativeLabel: "omitted-original-follower-close-observation" },
  ],
  "local-ready-snapshot-allocation": [
    "rust/crates/stream/src/local/local_stream.rs", "acyclic-stream",
    "local::snapshot_ready::ready_snapshots_allocate_zero_and_match_independent_prefixes",
    "ready snapshots must allocate zero times", ["--no-default-features", "--features", "test-support"],
    {
      mutations: [
        { patch: "rust/proofs/local-ready-snapshot-allocation/mutation.patch", label: "boxed-original-local-snapshot", diagnostic: "ready snapshots must allocate zero times" },
        { patch: "rust/proofs/local-failure-progress/observation.patch", label: "missing-observation-retirement", diagnostic: "failed journal left a public observation parked",
          command: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--features", "test-support", "--lib", "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers", "--", "--exact", "--nocapture"] },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--features", "test-support", "--lib", "--", "--exact", "--nocapture",
        "local::snapshot_ready::ready_snapshots_allocate_zero_and_match_independent_prefixes",
        "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers"],
      completed: ["2 passed; 0 failed"],
    },
  ],
  "trust-anchor-producer-progress": [
    "rust/crates/stream/src/trust_feed.rs", "acyclic-stream",
    "trust_anchor::placeholder_anchor_recovers_on_key_publication_without_restart",
    "lost anchor producer left its original trust feed parked", ["--features", "test-support"],
    {
      target: ["--test", "main"],
      mutations: [
        { patch: "rust/proofs/trust-anchor-producer-progress/retirement.patch", label: "discarded-anchor-producer-loss", diagnostic: "lost anchor producer left its original trust feed parked" },
        { patch: "rust/proofs/trust-anchor-producer-progress/cause.patch", label: "replaced-anchor-loss-cause", diagnostic: "lost anchor producer lost its original fence cause" },
      ],
    },
  ],
  "local-prepared-ingress-close": [
    "rust/crates/stream/src/native/prepared.rs", "acyclic-stream",
    "local::tests::native::abandoned_prepared_packet_releases_worker_capacity_without_edit",
    "abandoned prepared producer did not reach its original completion boundary", ["--no-default-features", ],
    {
      mutations: [
        { patch: "rust/proofs/local-prepared-ingress-close/mutation.patch", label: "omitted-original-ingress-close", diagnostic: "abandoned prepared producer did not reach its original completion boundary" },
        { patch: "rust/proofs/local-prepared-ingress-close/registration.patch", label: "omitted-original-ingress-registration", diagnostic: "abandoned prepared producer did not reach its original completion boundary" },
      ],
    },
  ],
  "local-prepared-producer-loss": [
    "rust/crates/stream/src/local/prepared_commit.rs", "acyclic-stream",
    "local::tests::native::prepared_worker_retirement_ends_original_completion_without_publication",
    "retired prepared producer left its original completion parked or replaced its cause", ["--no-default-features", ],
    {
      patchPath: "rust/proofs/local-prepared-producer-loss/mutation.patch",
      negativeLabel: "discarded-original-prepared-producer-error",
    },
  ],
  "local-snapshot-producer-loss": [
    "rust/crates/stream/src/snapshot.rs", "acyclic-stream",
    "local::tests::wakeups::executor_retirement_ends_forked_follows_before_publication_unlocks",
    "retired local snapshot left its public follow parked", ["--no-default-features", ],
    {
      mutations: [
        { source: "rust/crates/stream/src/snapshot/job.rs", patch: "rust/proofs/snapshot-producer-loss/retirement.patch", label: "omitted-local-producer-retirement", diagnostic: "retired local snapshot did not wake its public follow" },
        { patch: "rust/proofs/snapshot-producer-loss/retry.patch", label: "restarted-local-read-after-producer-loss", diagnostic: "retired local snapshot left its public follow parked" },
      ],
    },
  ],
  "snapshot-retired-executor": [
    "rust/crates/stream/src/snapshot/job.rs", "acyclic-stream",
    "memory::tests::runtime_shutdown::retired_executor_refuses_new_public_follows_before_writer_release",
    "retired executor left a newly admitted public follow parked", ["--no-default-features"],
    {
      patchPath: "rust/proofs/snapshot-producer-loss/retirement.patch",
      negativeLabel: "rejected-worker-discards-completion",
    },
  ],
  "snapshot-executor-admission": [
    "rust/crates/stream/src/snapshot.rs", "acyclic-stream",
    "memory::tests::runtime_shutdown::executor_loss_ends_public_follows_before_a_retained_writer_releases",
    "snapshot without an executor did not refuse before scheduling", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/snapshot-executor-admission/missing.patch", label: "omitted-executor-admission", diagnostic: "snapshot without an executor did not refuse before scheduling" },
        { patch: "rust/proofs/snapshot-executor-admission/ready.patch", label: "executor-required-before-ready-probe", diagnostic: "ready snapshot required an executor" },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib", "--", "--exact", "--nocapture",
        "memory::tests::runtime_shutdown::executor_loss_ends_public_follows_before_a_retained_writer_releases",
        "memory::tests::parking::snapshot_callbacks_retire_outside_the_original_job_mutex"],
      completed: ["2 passed; 0 failed"],
    },
  ],
  "snapshot-producer-loss": [
    "rust/crates/stream/src/snapshot.rs", "acyclic-stream",
    "memory::tests::runtime_shutdown::executor_loss_ends_public_follows_before_a_retained_writer_releases",
    "lost snapshot producer left its public follow parked", ["--no-default-features"],
    {
      mutations: [
        { source: "rust/crates/stream/src/snapshot/job.rs", patch: "rust/proofs/snapshot-producer-loss/retirement.patch", label: "omitted-producer-retirement", diagnostic: "lost snapshot producer did not wake its public follow" },
        { patch: "rust/proofs/snapshot-producer-loss/retry.patch", label: "restarted-read-after-producer-loss", diagnostic: "lost snapshot producer left its public follow parked" },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib", "--", "--exact", "--nocapture",
        "memory::tests::runtime_shutdown::executor_loss_ends_public_follows_before_a_retained_writer_releases",
        "memory::tests::parking::snapshot_callbacks_retire_outside_the_original_job_mutex"],
      completed: ["2 passed; 0 failed"],
    },
  ],
  "follow-index-reentry": [
    "rust/crates/stream/src/follow_index.rs", "acyclic-stream",
    "memory::tests::parking::publication_wakers_can_cancel_other_follows",
    "follow wake retained the index lock", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/follow-index-reentry/locked.patch", label: "notification-under-index-lock", diagnostic: "follow wake retained the index lock" },
        { patch: "rust/proofs/follow-index-reentry/omitted.patch", label: "missing-publication-wake", diagnostic: "publication left its follow parked" },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib", "--", "--exact", "--nocapture",
        "memory::tests::parking::publication_wakers_can_cancel_other_follows",
        "follow_index::tests::unrelated_publications_never_notify_filtered_interests",
        "follow_index::tests::interval_index_stays_balanced_and_matches_a_reference_after_removal"],
      completed: ["3 passed; 0 failed"],
    },
  ],
  "snapshot-waker-retirement": [
    "rust/crates/stream/src/snapshot/job.rs", "acyclic-stream",
    "memory::tests::parking::snapshot_callbacks_retire_outside_the_original_job_mutex",
    "wake must run outside the publication lock", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/snapshot-waker-retirement/replacement.patch", label: "retired-under-replacement-lock", diagnostic: "wake must run outside the publication lock" },
        { patch: "rust/proofs/snapshot-waker-retirement/cancellation.patch", label: "retired-under-cancellation-lock", diagnostic: "wake must run outside the publication lock" },
        { patch: "rust/proofs/snapshot-waker-retirement/completion.patch", label: "published-under-completion-lock", diagnostic: "wake must run outside the publication lock" },
      ],
    },
  ],
  "independent-terminal-observers": [
    "rust/crates/stream/src/operation_claim/termination.rs", "acyclic-stream",
    "pending::tests::commit_custody_keeps_exact_results_and_terminates_on_producer_loss",
    "retained pending observer blocked another observer's retirement", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/independent-terminal-observers/mutation.patch", label: "pending-observer-relay", diagnostic: "retained pending observer blocked another observer's retirement" },
        {
          patch: "rust/proofs/independent-terminal-observers/mutation.patch", label: "operation-observer-relay",
          diagnostic: "retained operation observer blocked another observer's retirement",
          command: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib",
            "operation_claim::termination::tests::independent_observers_survive_replacement_and_cancellation", "--", "--exact", "--nocapture"],
        },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib", "--", "--exact", "--nocapture",
        "pending::tests::commit_custody_keeps_exact_results_and_terminates_on_producer_loss",
        "operation_claim::termination::tests::independent_observers_survive_replacement_and_cancellation",
        "operation_claim::termination::tests::forgotten_producer_is_terminated_when_its_provider_owner_drops"],
      completed: ["3 passed; 0 failed"],
    },
  ],

  "revision-publication-progress": [
    "rust/crates/stream/src/lib/revision.rs", "acyclic-stream",
    "revision::tests::parks_until_applied_and_refuses_foreign_incarnations",
    "unchanged applied revision woke a parked reader", ["--no-default-features"],
  ],
  "revision-retirement-progress": [
    "rust/crates/stream/src/lib/revision.rs", "acyclic-stream",
    "revision::tests::parks_until_applied_and_refuses_foreign_incarnations",
    "retired revision authority did not wake its parked reader", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/revision-retirement-progress/notification.patch", label: "missing-retirement-wake", diagnostic: "retired revision authority did not wake its parked reader" },
        { patch: "rust/proofs/revision-retirement-progress/terminal.patch", label: "missing-terminal-state", diagnostic: "retired revision authority left its reader parked" },
      ],
    },
  ],
  "conditional-watch-parity": [
    "rust/crates/stream/src/query_sync/watch.rs", "acyclic-stream-publication-models",
    "watch_publication_cancellation_and_loss_match_tokio",
    "unmodified watch publication woke a parked reader", ["--features", "model-checking"],
    {
      target: ["--test", "publication"], loom: true,
      mutations: [
        { patch: "rust/proofs/conditional-watch-parity/mutation.patch", label: "mutated", diagnostic: "unmodified watch publication woke a parked reader" },
        { patch: "rust/proofs/conditional-watch-parity/owner.patch", label: "nonfinal-publisher-drop", diagnostic: "nonfinal watch publisher drop woke a parked reader" },
        { patch: "rust/proofs/conditional-watch-parity/cancellation.patch", label: "cancelled-registration", diagnostic: "cancelled watch wait retained its observer" },
        { patch: "rust/proofs/conditional-watch-parity/replacement.patch", label: "replaced-observer", diagnostic: "watch repoll retained its previous observer" },
        { patch: "rust/proofs/conditional-watch-parity/moved.patch", label: "moved-registration", diagnostic: "watch repoll retained its previous observer" },
      ],
    },
  ],
  "journal-cancellation-progress": [
    "rust/crates/stream/src/replicated.rs", "acyclic-cloudflare-conformance", "",
    "cancelled journal append left its public follow parked", [],
    {
      runner: ["node", "rust/proofs/journal-follow-progress/run.mjs", "--cancellation"],
      mutations: [
        { patch: "rust/proofs/journal-follow-progress/cancellation.patch", label: "missing-attempt-retirement", diagnostic: "cancelled journal append left its public follow parked" },
        { patch: "rust/proofs/journal-follow-progress/cancellation-revision.patch", label: "missing-cancelled-revision-retirement", diagnostic: "cancelled journal append left its public follow parked" },
      ],
      negativeExit: 1,
      failed: true,
      started: "journal public conditional cancellation START",
      completed: ["journal public conditional cancellation PASS"],
    },
  ],
  "journal-follow-progress": [
    "rust/crates/stream/src/journal_provider.rs", "acyclic-cloudflare-conformance", "",
    "journal failure left a public follow parked", [],
    {
      runner: ["node", "rust/proofs/journal-follow-progress/run.mjs"],
      negativeExit: 1,
      failed: true,
      completed: ["journal public follow retirement PASS"],
      mutations: [
        { patch: "rust/proofs/journal-follow-progress/retirement.patch", label: "missing-follow-retirement", diagnostic: "journal failure left a public follow parked" },
        { patch: "rust/proofs/journal-follow-progress/publication.patch", source: "rust/crates/stream/src/lib/revision.rs", label: "unrelated-publication-wake", diagnostic: "unrelated publication woke retirement observer" },
        { patch: "rust/proofs/journal-follow-progress/notification.patch", source: "rust/crates/stream/src/lib/revision.rs", label: "missing-retirement-notification", diagnostic: "journal failure left a public follow parked" },
      ],
    },
  ],
  "local-revision-failure-progress": [
    "rust/crates/stream/src/local/local_inner.rs", "acyclic-stream",
    "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers",
    "failed journal left a future revision parked", ["--features", "test-support"],
    { patch: "retirement.patch" },
  ],
  "local-failure-progress": [
    "rust/crates/stream/src/local/local_stream.rs", "acyclic-stream",
    "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers",
    "failed journal left a public observation parked", ["--features", "test-support"],
    {
      mutations: [
        { patch: "rust/proofs/local-failure-progress/publication.patch", label: "missing-publication-retirement", diagnostic: "failed journal left a conditional mutation parked" },
        { patch: "rust/proofs/local-failure-progress/observation.patch", label: "missing-observation-retirement", diagnostic: "failed journal left a public observation parked" },
        { patch: "rust/proofs/local-failure-progress/revision.patch", source: "rust/crates/stream/src/local/local_inner.rs", label: "missing-revision-retirement", diagnostic: "failed journal left a future revision parked" },
      ],
    },
  ],
  "local-native-failure-progress": [
    "rust/crates/stream/src/local/native_mutations.rs", "acyclic-stream",
    "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers",
    "failed journal left a native guarded commit parked", ["--features", "test-support"],
    {
      mutations: [
        { patch: "rust/proofs/local-failure-progress/guarded.patch", label: "missing-guarded-retirement", diagnostic: "failed journal left a native guarded commit parked" },
        { patch: "rust/proofs/local-failure-progress/preparation.patch", source: "rust/crates/stream/src/local/prepared_commit.rs", label: "missing-preparation-retirement", diagnostic: "failed journal left native preparation parked" },
        { patch: "rust/proofs/local-failure-progress/prepared.patch", source: "rust/crates/stream/src/local/prepared_commit.rs", label: "missing-prepared-retirement", diagnostic: "failed journal left a prepared completion parked" },
      ],
    },
  ],
  "browser-revision-progress": [
    "rust/crates/stream/src/browser/publication.rs", "acyclic-stream",
    "browser::tests::database_loss_ends_parked_revision_observers",
    "revision wait retained failed IndexedDB producer",
    ["--no-default-features", "--features", "wasm", "--target", "wasm32-unknown-unknown"],
  ],
  "postgres-refresh-progress": [
    "rust/crates/stream/src/postgres.rs", "acyclic-stream",
    "postgres::capability::refresh_producer_retirement_ends_public_waits",
    "retired refresh producer left a public revision wait parked", [],
    {
      mutations: [
        { patch: "rust/proofs/postgres-refresh-progress/retirement.patch", label: "missing-retirement", diagnostic: "retired refresh producer left a public revision wait parked" },
        { patch: "rust/proofs/postgres-refresh-progress/unpolled.patch", label: "late-retirement-custody", diagnostic: "retired refresh producer served a later observer" },
        { patch: "rust/proofs/postgres-refresh-progress/connection.patch", label: "missing-connection-retirement", diagnostic: "retired refresh producer left a public revision wait parked" },
        { patch: "rust/proofs/postgres-refresh-progress/session.patch", label: "missing-session-retirement", diagnostic: "retired producer left a public session refresh parked" },
        { patch: "rust/proofs/postgres-refresh-progress/publication.patch", label: "missing-publication-retirement", diagnostic: "retired producer left a conditional publication parked" },
      ],
    },
  ],
  "backed-retention": [
    "rust/crates/stream/src/owned_bytes/mod.rs", "acyclic-stream",
    "owned_bytes::tests::backed_retention_preserves_visible_bytes_identity_and_final_custody",
    "spare backing escaped compaction", [],
    {
      mutations: [
        { patch: "rust/proofs/backed-retention/compaction.patch", label: "missing-compaction", diagnostic: "spare backing escaped compaction" },
        { patch: "rust/proofs/backed-retention/known-copy.patch", label: "copied-known-owner", diagnostic: "known backing was copied" },
        { patch: "rust/proofs/backed-retention/custody.patch", label: "lost-backing-custody", diagnostic: "backing lost original owner" },
        { patch: "rust/proofs/backed-retention/static-capacity.patch", label: "unknown-static-capacity", diagnostic: "known backing was copied" },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "-p", "acyclic-harness-client", "--lib", "--", "--exact", "--nocapture",
        "owned_bytes::tests::backed_retention_preserves_visible_bytes_identity_and_final_custody",
        "owned_bytes::tests::ordinary_storage_preserves_capacity_and_rejects_unsafe_recovery",
        "reader::cache::tests::tiny_slices_detach_unknown_or_over_budget_transport_backing"],
      completed: [
        "test owned_bytes::tests::backed_retention_preserves_visible_bytes_identity_and_final_custody ... ok",
        "test owned_bytes::tests::ordinary_storage_preserves_capacity_and_rejects_unsafe_recovery ... ok",
        "test reader::cache::tests::tiny_slices_detach_unknown_or_over_budget_transport_backing ... ok",
      ],
    },
  ],
  "compiled-prefix-invariant": [
    "rust/crates/collections/src/compiled_prefix.rs", "acyclic-collections",
    "tests::compiled_prefix::compiled_prefix_cost_and_backing_do_not_grow_with_unrelated_keys",
    "changed original prefix label became absence", [],
    {
      positiveCommand: ["test", "--locked", "-p", "acyclic-collections", "-p", "acyclic-native-runtime", "--lib", "--", "--exact", "--nocapture",
        "tests::compiled_prefix::compiled_prefix_matches_original_keys_and_refuses_original_read_credit",
        "tests::compiled_prefix::compiled_prefix_cost_and_backing_do_not_grow_with_unrelated_keys",
        "execution::tests::compiled_prefix_grant_proofs_use_original_operation_and_allocation_credits"],
      completed: ["2 passed; 0 failed", "1 passed; 0 failed"],
    },
  ],
  "actors-conformance-clock": [
    "rust/crates/actors/src/conformance.rs", "acyclic-actors",
    "local::runtime::tests::conformance_suite_passes_on_memory_runtime",
    "outbox", [],
    { mutations: [
      { patch: "rust/proofs/actors-conformance-clock/early.patch", label: "alarm-before-observation", diagnostic: "outbox" },
      { patch: "rust/proofs/actors-conformance-clock/missing.patch", label: "restart-without-advancement", diagnostic: "outbox" },
    ] },
  ],
  "actors-alarm-deadline": [
    "rust/crates/actors/src/local/runtime.rs", "acyclic-actors",
    "local::runtime::tests::conformance_suite_passes_on_memory_runtime",
    "CompletionFailed(Conflict)", [],
  ],
  "call-dependent-resource": [
    "rust/crates/stream/src/session/resources.rs", "acyclic-stream",
    "session::tests::calls::dependent_resources_keep_original_call_grants_and_live_trust",
    "dependent scope escaped its original declared boundary", ["--features", "test-support"],
    {
      mutations: [
        { patch: "rust/proofs/call-dependent-resource/scope.patch", label: "discarded-dependent-resource-boundary", diagnostic: "dependent scope escaped its original declared boundary" },
        { patch: "rust/proofs/call-dependent-resource/credential.patch", label: "detached-original-current-credential", diagnostic: "dependent resource detached from the original credential renewal" },
        { patch: "rust/proofs/call-dependent-resource/policy.patch", label: "discarded-dependent-owner-policy", diagnostic: "dependent disclosure ignored the owner's current policy" },
        { patch: "rust/proofs/call-dependent-resource/index.patch", label: "discarded-dependent-index-normalization", diagnostic: "resource index differs from its original authorized scope" },
      ],
    },
  ],
  "config-composition-cut": [
    "rust/crates/harness-core/src/config/evaluate/composition.rs", "acyclic-harness-core",
    "config::evaluate::model::tests::generated_settings_match_fresh_evaluation",
    "composition discarded its original refused module cut", [],
    {
      mutations: [
        { patch: "rust/proofs/config-composition-cut/refusal.patch", label: "discarded-original-refused-module-cut", diagnostic: "composition discarded its original refused module cut" },
        { patch: "rust/proofs/config-composition-cut/generations.patch", label: "discarded-original-layer-generations", diagnostic: "composition identity discarded its original layer generations" },
      ],
    },
  ],
  "config-retained-cut": [
    "rust/crates/harness-core/src/config/evaluate/prepared.rs", "acyclic-harness-core",
    "config::evaluate::model::tests::generated_settings_match_fresh_evaluation",
    "original dependency observation was discarded", [],
    {
      mutations: [
        { patch: "rust/proofs/config-retained-cut/absence.patch", label: "discarded-original-absent-import", diagnostic: "original dependency observation was discarded" },
        { patch: "rust/proofs/config-retained-cut/identity.patch", label: "discarded-original-read-cut-identity", diagnostic: "settings input identity aliased a different dependency cut" },
      ],
    },
  ],
  "trust-fenced-close-progress": [
    "rust/crates/stream/src/trust_feed.rs", "acyclic-stream",
    "trust_anchor::closing_a_fenced_environment_ends_its_original_anchor_wait",
    "closed follower left its original anchor wait parked", ["--features", "test-support"],
    { target: ["--test", "main"], patchPath: "rust/proofs/trust-fenced-close-progress/mutation.patch", negativeLabel: "omitted-original-follower-close-observation" },
  ],
  "local-ready-snapshot-allocation": [
    "rust/crates/stream/src/local/local_stream.rs", "acyclic-stream",
    "local::snapshot_ready::ready_snapshots_allocate_zero_and_match_independent_prefixes",
    "ready snapshots must allocate zero times", ["--no-default-features", "--features", "test-support"],
    {
      mutations: [
        { patch: "rust/proofs/local-ready-snapshot-allocation/mutation.patch", label: "boxed-original-local-snapshot", diagnostic: "ready snapshots must allocate zero times" },
        { patch: "rust/proofs/local-failure-progress/observation.patch", label: "missing-observation-retirement", diagnostic: "failed journal left a public observation parked",
          command: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--features", "test-support", "--lib", "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers", "--", "--exact", "--nocapture"] },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--features", "test-support", "--lib", "--", "--exact", "--nocapture",
        "local::snapshot_ready::ready_snapshots_allocate_zero_and_match_independent_prefixes",
        "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers"],
      completed: ["2 passed; 0 failed"],
    },
  ],
  "trust-anchor-producer-progress": [
    "rust/crates/stream/src/trust_feed.rs", "acyclic-stream",
    "trust_anchor::placeholder_anchor_recovers_on_key_publication_without_restart",
    "lost anchor producer left its original trust feed parked", ["--features", "test-support"],
    {
      target: ["--test", "main"],
      mutations: [
        { patch: "rust/proofs/trust-anchor-producer-progress/retirement.patch", label: "discarded-anchor-producer-loss", diagnostic: "lost anchor producer left its original trust feed parked" },
        { patch: "rust/proofs/trust-anchor-producer-progress/cause.patch", label: "replaced-anchor-loss-cause", diagnostic: "lost anchor producer lost its original fence cause" },
      ],
    },
  ],
  "local-prepared-ingress-close": [
    "rust/crates/stream/src/native/prepared.rs", "acyclic-stream",
    "local::tests::native::abandoned_prepared_packet_releases_worker_capacity_without_edit",
    "abandoned prepared producer did not reach its original completion boundary", ["--no-default-features", ],
    {
      mutations: [
        { patch: "rust/proofs/local-prepared-ingress-close/mutation.patch", label: "omitted-original-ingress-close", diagnostic: "abandoned prepared producer did not reach its original completion boundary" },
        { patch: "rust/proofs/local-prepared-ingress-close/registration.patch", label: "omitted-original-ingress-registration", diagnostic: "abandoned prepared producer did not reach its original completion boundary" },
      ],
    },
  ],
  "local-prepared-producer-loss": [
    "rust/crates/stream/src/local/prepared_commit.rs", "acyclic-stream",
    "local::tests::native::prepared_worker_retirement_ends_original_completion_without_publication",
    "retired prepared producer left its original completion parked or replaced its cause", ["--no-default-features", ],
    {
      patchPath: "rust/proofs/local-prepared-producer-loss/mutation.patch",
      negativeLabel: "discarded-original-prepared-producer-error",
    },
  ],
  "local-snapshot-producer-loss": [
    "rust/crates/stream/src/snapshot.rs", "acyclic-stream",
    "local::tests::wakeups::executor_retirement_ends_forked_follows_before_publication_unlocks",
    "retired local snapshot left its public follow parked", ["--no-default-features", ],
    {
      mutations: [
        { source: "rust/crates/stream/src/snapshot/job.rs", patch: "rust/proofs/snapshot-producer-loss/retirement.patch", label: "omitted-local-producer-retirement", diagnostic: "retired local snapshot did not wake its public follow" },
        { patch: "rust/proofs/snapshot-producer-loss/retry.patch", label: "restarted-local-read-after-producer-loss", diagnostic: "retired local snapshot left its public follow parked" },
      ],
    },
  ],
  "snapshot-retired-executor": [
    "rust/crates/stream/src/snapshot/job.rs", "acyclic-stream",
    "memory::tests::runtime_shutdown::retired_executor_refuses_new_public_follows_before_writer_release",
    "retired executor left a newly admitted public follow parked", ["--no-default-features"],
    {
      patchPath: "rust/proofs/snapshot-producer-loss/retirement.patch",
      negativeLabel: "rejected-worker-discards-completion",
    },
  ],
  "snapshot-executor-admission": [
    "rust/crates/stream/src/snapshot.rs", "acyclic-stream",
    "memory::tests::runtime_shutdown::executor_loss_ends_public_follows_before_a_retained_writer_releases",
    "snapshot without an executor did not refuse before scheduling", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/snapshot-executor-admission/missing.patch", label: "omitted-executor-admission", diagnostic: "snapshot without an executor did not refuse before scheduling" },
        { patch: "rust/proofs/snapshot-executor-admission/ready.patch", label: "executor-required-before-ready-probe", diagnostic: "ready snapshot required an executor" },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib", "--", "--exact", "--nocapture",
        "memory::tests::runtime_shutdown::executor_loss_ends_public_follows_before_a_retained_writer_releases",
        "memory::tests::parking::snapshot_callbacks_retire_outside_the_original_job_mutex"],
      completed: ["2 passed; 0 failed"],
    },
  ],
  "snapshot-producer-loss": [
    "rust/crates/stream/src/snapshot.rs", "acyclic-stream",
    "memory::tests::runtime_shutdown::executor_loss_ends_public_follows_before_a_retained_writer_releases",
    "lost snapshot producer left its public follow parked", ["--no-default-features"],
    {
      mutations: [
        { source: "rust/crates/stream/src/snapshot/job.rs", patch: "rust/proofs/snapshot-producer-loss/retirement.patch", label: "omitted-producer-retirement", diagnostic: "lost snapshot producer did not wake its public follow" },
        { patch: "rust/proofs/snapshot-producer-loss/retry.patch", label: "restarted-read-after-producer-loss", diagnostic: "lost snapshot producer left its public follow parked" },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib", "--", "--exact", "--nocapture",
        "memory::tests::runtime_shutdown::executor_loss_ends_public_follows_before_a_retained_writer_releases",
        "memory::tests::parking::snapshot_callbacks_retire_outside_the_original_job_mutex"],
      completed: ["2 passed; 0 failed"],
    },
  ],
  "follow-index-reentry": [
    "rust/crates/stream/src/follow_index.rs", "acyclic-stream",
    "memory::tests::parking::publication_wakers_can_cancel_other_follows",
    "follow wake retained the index lock", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/follow-index-reentry/locked.patch", label: "notification-under-index-lock", diagnostic: "follow wake retained the index lock" },
        { patch: "rust/proofs/follow-index-reentry/omitted.patch", label: "missing-publication-wake", diagnostic: "publication left its follow parked" },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib", "--", "--exact", "--nocapture",
        "memory::tests::parking::publication_wakers_can_cancel_other_follows",
        "follow_index::tests::unrelated_publications_never_notify_filtered_interests",
        "follow_index::tests::interval_index_stays_balanced_and_matches_a_reference_after_removal"],
      completed: ["3 passed; 0 failed"],
    },
  ],
  "snapshot-waker-retirement": [
    "rust/crates/stream/src/snapshot/job.rs", "acyclic-stream",
    "memory::tests::parking::snapshot_callbacks_retire_outside_the_original_job_mutex",
    "wake must run outside the publication lock", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/snapshot-waker-retirement/replacement.patch", label: "retired-under-replacement-lock", diagnostic: "wake must run outside the publication lock" },
        { patch: "rust/proofs/snapshot-waker-retirement/cancellation.patch", label: "retired-under-cancellation-lock", diagnostic: "wake must run outside the publication lock" },
        { patch: "rust/proofs/snapshot-waker-retirement/completion.patch", label: "published-under-completion-lock", diagnostic: "wake must run outside the publication lock" },
      ],
    },
  ],
  "independent-terminal-observers": [
    "rust/crates/stream/src/operation_claim/termination.rs", "acyclic-stream",
    "pending::tests::commit_custody_keeps_exact_results_and_terminates_on_producer_loss",
    "retained pending observer blocked another observer's retirement", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/independent-terminal-observers/mutation.patch", label: "pending-observer-relay", diagnostic: "retained pending observer blocked another observer's retirement" },
        {
          patch: "rust/proofs/independent-terminal-observers/mutation.patch", label: "operation-observer-relay",
          diagnostic: "retained operation observer blocked another observer's retirement",
          command: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib",
            "operation_claim::termination::tests::independent_observers_survive_replacement_and_cancellation", "--", "--exact", "--nocapture"],
        },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--lib", "--", "--exact", "--nocapture",
        "pending::tests::commit_custody_keeps_exact_results_and_terminates_on_producer_loss",
        "operation_claim::termination::tests::independent_observers_survive_replacement_and_cancellation",
        "operation_claim::termination::tests::forgotten_producer_is_terminated_when_its_provider_owner_drops"],
      completed: ["3 passed; 0 failed"],
    },
  ],

  "revision-publication-progress": [
    "rust/crates/stream/src/lib/revision.rs", "acyclic-stream",
    "revision::tests::parks_until_applied_and_refuses_foreign_incarnations",
    "unchanged applied revision woke a parked reader", ["--no-default-features"],
  ],
  "revision-retirement-progress": [
    "rust/crates/stream/src/lib/revision.rs", "acyclic-stream",
    "revision::tests::parks_until_applied_and_refuses_foreign_incarnations",
    "retired revision authority did not wake its parked reader", ["--no-default-features"],
    {
      mutations: [
        { patch: "rust/proofs/revision-retirement-progress/notification.patch", label: "missing-retirement-wake", diagnostic: "retired revision authority did not wake its parked reader" },
        { patch: "rust/proofs/revision-retirement-progress/terminal.patch", label: "missing-terminal-state", diagnostic: "retired revision authority left its reader parked" },
      ],
    },
  ],
  "conditional-watch-parity": [
    "rust/crates/stream/src/query_sync/watch.rs", "acyclic-stream-publication-models",
    "watch_publication_cancellation_and_loss_match_tokio",
    "unmodified watch publication woke a parked reader", ["--features", "model-checking"],
    {
      target: ["--test", "publication"], loom: true,
      mutations: [
        { patch: "rust/proofs/conditional-watch-parity/mutation.patch", label: "mutated", diagnostic: "unmodified watch publication woke a parked reader" },
        { patch: "rust/proofs/conditional-watch-parity/owner.patch", label: "nonfinal-publisher-drop", diagnostic: "nonfinal watch publisher drop woke a parked reader" },
        { patch: "rust/proofs/conditional-watch-parity/cancellation.patch", label: "cancelled-registration", diagnostic: "cancelled watch wait retained its observer" },
        { patch: "rust/proofs/conditional-watch-parity/replacement.patch", label: "replaced-observer", diagnostic: "watch repoll retained its previous observer" },
        { patch: "rust/proofs/conditional-watch-parity/moved.patch", label: "moved-registration", diagnostic: "watch repoll retained its previous observer" },
      ],
    },
  ],
  "journal-cancellation-progress": [
    "rust/crates/stream/src/replicated.rs", "acyclic-cloudflare-conformance", "",
    "cancelled journal append left its public follow parked", [],
    {
      runner: ["node", "rust/proofs/journal-follow-progress/run.mjs", "--cancellation"],
      mutations: [
        { patch: "rust/proofs/journal-follow-progress/cancellation.patch", label: "missing-attempt-retirement", diagnostic: "cancelled journal append left its public follow parked" },
        { patch: "rust/proofs/journal-follow-progress/cancellation-revision.patch", label: "missing-cancelled-revision-retirement", diagnostic: "cancelled journal append left its public follow parked" },
      ],
      negativeExit: 1,
      failed: true,
      started: "journal public conditional cancellation START",
      completed: ["journal public conditional cancellation PASS"],
    },
  ],
  "journal-follow-progress": [
    "rust/crates/stream/src/journal_provider.rs", "acyclic-cloudflare-conformance", "",
    "journal failure left a public follow parked", [],
    {
      runner: ["node", "rust/proofs/journal-follow-progress/run.mjs"],
      negativeExit: 1,
      failed: true,
      completed: ["journal public follow retirement PASS"],
      mutations: [
        { patch: "rust/proofs/journal-follow-progress/retirement.patch", label: "missing-follow-retirement", diagnostic: "journal failure left a public follow parked" },
        { patch: "rust/proofs/journal-follow-progress/publication.patch", source: "rust/crates/stream/src/lib/revision.rs", label: "unrelated-publication-wake", diagnostic: "unrelated publication woke retirement observer" },
        { patch: "rust/proofs/journal-follow-progress/notification.patch", source: "rust/crates/stream/src/lib/revision.rs", label: "missing-retirement-notification", diagnostic: "journal failure left a public follow parked" },
      ],
    },
  ],
  "local-revision-failure-progress": [
    "rust/crates/stream/src/local/local_inner.rs", "acyclic-stream",
    "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers",
    "failed journal left a future revision parked", ["--features", "test-support"],
    { patch: "retirement.patch" },
  ],
  "local-failure-progress": [
    "rust/crates/stream/src/local/local_stream.rs", "acyclic-stream",
    "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers",
    "failed journal left a public observation parked", ["--features", "test-support"],
    {
      mutations: [
        { patch: "rust/proofs/local-failure-progress/publication.patch", label: "missing-publication-retirement", diagnostic: "failed journal left a conditional mutation parked" },
        { patch: "rust/proofs/local-failure-progress/observation.patch", label: "missing-observation-retirement", diagnostic: "failed journal left a public observation parked" },
        { patch: "rust/proofs/local-failure-progress/revision.patch", source: "rust/crates/stream/src/local/local_inner.rs", label: "missing-revision-retirement", diagnostic: "failed journal left a future revision parked" },
      ],
    },
  ],
  "local-native-failure-progress": [
    "rust/crates/stream/src/local/native_mutations.rs", "acyclic-stream",
    "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers",
    "failed journal left a native guarded commit parked", ["--features", "test-support"],
    {
      mutations: [
        { patch: "rust/proofs/local-failure-progress/guarded.patch", label: "missing-guarded-retirement", diagnostic: "failed journal left a native guarded commit parked" },
        { patch: "rust/proofs/local-failure-progress/preparation.patch", source: "rust/crates/stream/src/local/prepared_commit.rs", label: "missing-preparation-retirement", diagnostic: "failed journal left native preparation parked" },
        { patch: "rust/proofs/local-failure-progress/prepared.patch", source: "rust/crates/stream/src/local/prepared_commit.rs", label: "missing-prepared-retirement", diagnostic: "failed journal left a prepared completion parked" },
      ],
    },
  ],
  "browser-revision-progress": [
    "rust/crates/stream/src/browser/publication.rs", "acyclic-stream",
    "browser::tests::database_loss_ends_parked_revision_observers",
    "revision wait retained failed IndexedDB producer",
    ["--no-default-features", "--features", "wasm", "--target", "wasm32-unknown-unknown"],
  ],
  "postgres-refresh-progress": [
    "rust/crates/stream/src/postgres.rs", "acyclic-stream",
    "postgres::capability::refresh_producer_retirement_ends_public_waits",
    "retired refresh producer left a public revision wait parked", [],
    {
      mutations: [
        { patch: "rust/proofs/postgres-refresh-progress/retirement.patch", label: "missing-retirement", diagnostic: "retired refresh producer left a public revision wait parked" },
        { patch: "rust/proofs/postgres-refresh-progress/unpolled.patch", label: "late-retirement-custody", diagnostic: "retired refresh producer served a later observer" },
        { patch: "rust/proofs/postgres-refresh-progress/connection.patch", label: "missing-connection-retirement", diagnostic: "retired refresh producer left a public revision wait parked" },
        { patch: "rust/proofs/postgres-refresh-progress/session.patch", label: "missing-session-retirement", diagnostic: "retired producer left a public session refresh parked" },
        { patch: "rust/proofs/postgres-refresh-progress/publication.patch", label: "missing-publication-retirement", diagnostic: "retired producer left a conditional publication parked" },
      ],
    },
  ],
  "backed-retention": [
    "rust/crates/stream/src/owned_bytes/mod.rs", "acyclic-stream",
    "owned_bytes::tests::backed_retention_preserves_visible_bytes_identity_and_final_custody",
    "spare backing escaped compaction", [],
    {
      mutations: [
        { patch: "rust/proofs/backed-retention/compaction.patch", label: "missing-compaction", diagnostic: "spare backing escaped compaction" },
        { patch: "rust/proofs/backed-retention/known-copy.patch", label: "copied-known-owner", diagnostic: "known backing was copied" },
        { patch: "rust/proofs/backed-retention/custody.patch", label: "lost-backing-custody", diagnostic: "backing lost original owner" },
        { patch: "rust/proofs/backed-retention/static-capacity.patch", label: "unknown-static-capacity", diagnostic: "known backing was copied" },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "-p", "acyclic-harness-client", "--lib", "--", "--exact", "--nocapture",
        "owned_bytes::tests::backed_retention_preserves_visible_bytes_identity_and_final_custody",
        "owned_bytes::tests::ordinary_storage_preserves_capacity_and_rejects_unsafe_recovery",
        "reader::cache::tests::tiny_slices_detach_unknown_or_over_budget_transport_backing"],
      completed: [
        "test owned_bytes::tests::backed_retention_preserves_visible_bytes_identity_and_final_custody ... ok",
        "test owned_bytes::tests::ordinary_storage_preserves_capacity_and_rejects_unsafe_recovery ... ok",
        "test reader::cache::tests::tiny_slices_detach_unknown_or_over_budget_transport_backing ... ok",
      ],
    },
  ],
  "compiled-prefix-invariant": [
    "rust/crates/collections/src/compiled_prefix.rs", "acyclic-collections",
    "tests::compiled_prefix::compiled_prefix_cost_and_backing_do_not_grow_with_unrelated_keys",
    "changed original prefix label became absence", [],
    {
      positiveCommand: ["test", "--locked", "-p", "acyclic-collections", "-p", "acyclic-native-runtime", "--lib", "--", "--exact", "--nocapture",
        "tests::compiled_prefix::compiled_prefix_matches_original_keys_and_refuses_original_read_credit",
        "tests::compiled_prefix::compiled_prefix_cost_and_backing_do_not_grow_with_unrelated_keys",
        "execution::tests::compiled_prefix_grant_proofs_use_original_operation_and_allocation_credits"],
      completed: ["2 passed; 0 failed", "1 passed; 0 failed"],
    },
  ],
  "failed-input-fairness": [
    "rust/crates/actors/src/local/runtime.rs", "acyclic-actors",
    "local::runtime::tests::retry::a_failed_record_does_not_starve_other_ready_subscriptions",
    "failed input starved another ready subscription", [],
    { positive: [
      "local::runtime::tests::retry::a_failed_record_does_not_starve_other_ready_subscriptions",
      "local::runtime::tests::retry::guard_refusals_distinguish_unchanged_observations_from_source_changes",
    ] },
  ],
  "seeded-runtime-configuration": [
    "rust/crates/native-runtime/src/testing.rs", "acyclic-native-runtime",
    "seeded_runtime_requires_explicit_executor_configuration",
    "unconfigured replay admitted an unseeded executor", ["--features", "test-support"],
    {
      target: ["--test", "seeded_runtime"],
      // This claim exercises refusal when Tokio's unstable seed API is absent.
      env: { RUSTFLAGS: "-Dunexpected_cfgs", CARGO_ENCODED_RUSTFLAGS: "-Dunexpected_cfgs" },
    },
  ],
  "merge-directory-custody": [
    "rust/crates/filesystem/src/workspace/join_directory.rs", "acyclic-fs",
    "workspace::tests::merge_drivers::merge_directory_pages_preserve_pins_and_non_text_conflicts",
    "a one-row page expanded the whole merge", [],
    { mutations: [
      { patch: "rust/proofs/merge-directory-custody/foreign-generation.patch", label: "foreign-generation-admitted", diagnostic: "directory.resolution(&foreign, binary_key)" },
      { patch: "rust/proofs/merge-directory-custody/page-credit.patch", label: "page-credit-exceeded", diagnostic: "a one-row page expanded the whole merge" },
    ] },
  ],
  "merge-text-admission": [
    "rust/crates/filesystem/src/workspace/join_directory.rs", "acyclic-fs-fuzz", "",
    "captured public merge resolution was refused", ["--features", "fuzz-targets"],
    {
      fuzz: { bin: "merge_directory_resolution", corpus: "rust/crates/filesystem/fuzz/corpus/merge_directory_resolution", runs: 1024 },
      mutations: [
        { patch: "rust/proofs/merge-text-admission/refusal.patch", label: "text-proposal-refused", diagnostic: "captured public merge resolution was refused" },
        { patch: "rust/proofs/merge-text-admission/markers.patch", label: "unresolved-markers-admitted", diagnostic: "captured public merge refusal changed" },
        { source: "rust/crates/filesystem/src/merge_driver.rs", patch: "rust/proofs/merge-text-admission/wire.patch", label: "text-wire-type-substituted", diagnostic: "captured public text encoding changed" },
      ],
    },
  ],
  "merge-directory-admission": [
    "rust/crates/filesystem/src/workspace/join_directory.rs", "acyclic-fs-fuzz", "",
    "captured merge-directory writer output was refused", ["--features", "fuzz-targets"],
    {
      fuzz: { bin: "merge_directory_resolution", corpus: "rust/crates/filesystem/fuzz/corpus/merge_directory_resolution", runs: 1024 },
      mutations: [
        { patch: "rust/proofs/merge-directory-admission/admission.patch", label: "writer-output-refused", diagnostic: "captured merge-directory writer output was refused" },
        { patch: "rust/proofs/merge-directory-admission/version.patch", label: "future-version-admitted", diagnostic: "unsupported merge file version accepted by the shipped decoder" },
        { patch: "rust/proofs/merge-directory-admission/resolution-refusal.patch", label: "caller-resolution-refused", diagnostic: "captured public merge resolution was refused" },
        { patch: "rust/proofs/merge-directory-admission/resolution-side.patch", label: "caller-side-substituted", diagnostic: "captured public merge resolution changed" },
      ],
    },
  ],
  "native-hook-ownership": [
    "rust/crates/harness/src/host_hooks.rs", "acyclic-sdk-fuzz", "",
    "foreign hook metadata was admitted", [],
    { fuzz: { bin: "native_hooks", corpus: "rust/fuzz/corpus/native_hooks", runs: 1024 } },
  ],
  "native-hook-admission": [
    "rust/crates/harness/src/host_hooks.rs", "acyclic-sdk-fuzz", "",
    "native hook writer output was refused", [],
    {
      fuzz: { bin: "native_hooks", corpus: "rust/fuzz/corpus/native_hooks", runs: 1024 },
      mutations: [
        { patch: "rust/proofs/native-hook-admission/version-refusal.patch", label: "supported-version-refused", diagnostic: "captured supported Codex version was refused" },
        { patch: "rust/proofs/native-hook-admission/version-admission.patch", label: "obsolete-version-admitted", diagnostic: "captured obsolete Codex version was admitted" },
        { patch: "rust/proofs/native-hook-admission/ownership.patch", label: "writer-ownership-refused", diagnostic: "native hook writer output was refused" },
      ],
    },
  ],
  "approval-wake-uncertainty": [
    "rust/crates/harness-core/src/filesystem/agent_turns.rs", "acyclic-agent-host-conformance",
    "local::local_approval_wake_retries_the_original_uncertain_request",
    "approval wake uncertainty lost its original request", [],
    { target: ["--test", "main"] },
  ],
  "filesystem-stream-category": [
    "rust/crates/filesystem/src/wire_service/errors.rs", "acyclic-fs",
    "wire_service::errors::tests::every_stream_category_survives_the_authority_wrapper",
    "filesystem retained Stream refusal changed category", ["--no-default-features", "--features", "memory"],
    { oracle: "rust/crates/filesystem/src/wire_service/errors/tests.rs",
      nextest: ["--profile", "ci", "--no-fail-fast", "--retries", "0", "--no-tests=fail"] },
  ],
  "session-local-refusal": [
    "rust/crates/stream/src/session/frame.rs", "acyclic-stream",
    "session::tests::refusals::local_socket_refusals_retain_the_original_error_after_prior_data",
    "local socket lost its original refusal", ["--features", "serde,test-support"],
    { oracle: "rust/crates/stream/src/session/tests/refusals.rs",
      nextest: ["--profile", "ci", "--no-fail-fast", "--retries", "0", "--no-tests=fail"],
      positive: [
        "session::tests::refusals::local_socket_refusals_retain_the_original_error_after_prior_data",
        "session::tests::sockets::receive_refusal_releases_owner_before_uncredited_frames_drain",
        "session::tests::sockets::host_shutdown_closes_all_channels_without_data_credit",
        "session::tests::sockets::a_socket_the_host_ends_after_its_final_frame_ends_cleanly_for_the_peer",
        "session::tests::queue_scaling::credited_tail_selection_visits_one_head_independent_of_stalled_follows",
      ] },
  ],
  "stream-error-admission": [
    "rust/crates/stream/src/lib/error.rs", "acyclic-guest-fuzz", "",
    "canonical failure writer output was refused", [],
    { fuzz: { bin: "stream_error", corpus: "fuzz/corpus/stream_error", runs: 1024 } },
  ],
  "invocation-header-admission": [
    "rust/crates/actors/src/domain/invocation.rs", "acyclic-guest-fuzz", "",
    "captured actor invocation was refused", [],
    { fuzz: { bin: "invocation_headers", corpus: "fuzz/corpus/invocation_headers", runs: 1024 } },
  ],
  "dispatcher-clock-retirement": [
    "rust/crates/actors/src/local/runtime.rs", "acyclic-actors",
    "local::runtime::tests::clock::running_dispatcher_reports_its_original_clock_retirement",
    "dispatcher did not return its original retired clock failure", [],
  ],
  "system-clock-runtime-retirement": [
    "rust/crates/actors/src/local/runtime/clock.rs", "acyclic-actors",
    "local::runtime::tests::clock::system_clock_keeps_its_original_deadline_after_runtime_retirement",
    "wall clock retained a retired Tokio timer driver", [],
    { mutations: [
      { patch: "rust/proofs/system-clock-runtime-retirement/mutation.patch", label: "retired-runtime-driver", diagnostic: "wall clock retained a retired Tokio timer driver" },
      { patch: "rust/proofs/system-clock-runtime-retirement/deferred.patch", label: "deferred-clock-capture", diagnostic: "wall clock escaped its original producer before first poll" },
      { source: "rust/crates/native-runtime/src/wall_alarm/wait.rs", patch: "rust/proofs/system-clock-runtime-retirement/failure.patch", label: "masked-native-failure", diagnostic: "wall clock did not report original producer retirement" },
    ] },
  ],
  "ready-alarm-progress": [
    "rust/crates/actors/src/local/ready.rs", "acyclic-actors",
    "local::ready::tests::earlier_deadline_rearms_the_shared_dispatcher_after_unlock",
    "an earlier committed deadline must rearm the shared dispatcher", [],
  ],
  "ready-waker-custody": [
    "rust/crates/actors/src/local/ready.rs", "acyclic-actors",
    "local::ready::tests::earlier_deadline_rearms_the_shared_dispatcher_after_unlock",
    "dispatcher observer retirement retained the ready index lock", [],
    { mutations: [
      { patch: "rust/proofs/ready-waker-custody/poll.patch", label: "poll-ready-locked-retirement", diagnostic: "dispatcher observer retirement retained the ready index lock" },
      { patch: "rust/proofs/ready-waker-custody/listen.patch", label: "listen-locked-retirement", diagnostic: "dispatcher observer retirement retained the ready index lock" },
    ] },
  ],
  "settlement-retired-source": [
    "rust/crates/harness/src/local/observer.rs", "acyclic-harness",
    "local::tests::agents::lost_model_producer_closes_local_session_without_settling_input",
    "settlement retained a retired status source", nativeHarness,
    { mutations: [
      { patch: "rust/proofs/settlement-retired-source/status.patch", label: "retired-status", diagnostic: "settlement retained a retired status source" },
      { patch: "rust/proofs/settlement-retired-source/conversation.patch", label: "retired-conversation", diagnostic: "settlement retained a retired conversation source" },
    ] },
  ],
  "inbox-producer-admission": [
    "rust/crates/harness-core/src/agent/reply.rs", "acyclic-harness-core",
    "agent_model::runtime::reply_waits_require_original_committed_requests",
    "reply wait admitted an absent or mismatched original input",
    ["--features", "test-support"],
    { target: ["--test", "main"], positiveCommand: [
      "nextest", "run", "--locked", "-p", "acyclic-harness-core",
      "--features", "test-support", "--test", "main",
      "-E", "test(=agent_model::runtime::reply_waits_require_original_committed_requests) | test(=agent_model::reply::committed_reply_search_respects_identity_and_fork_base) | test(=agent_model::reply::admitted_steering_joins_its_settling_turn_and_original_identity)",
      "--no-tests=fail", "--retries", "0",
    ], completed: ["3 tests run: 3 passed"], mutations: [
      { patch: "rust/proofs/inbox-producer-admission/mutation.patch", label: "uncommitted-reply", diagnostic: "reply wait admitted an absent or mismatched original input" },
      { source: "rust/crates/harness-core/src/agent/host.rs", patch: "rust/proofs/inbox-producer-admission/spawn.patch", label: "uncommitted-spawn", diagnostic: "spawn wait admitted an absent or non-spawn original input" },
    ] },
  ],
  "public-pause-retirement": [
    "rust/crates/actors/src/local/runtime/pause.rs", "acyclic-harness-core",
    "target_lifecycle::native::abandoned_pause_joins_original_native_retirement",
    "abandoned public pause lost its original native transition",
    ["--features", "test-support"],
    { target: ["--test", "main"] },
  ],
  "public-settlement-source": [
    "rust/crates/harness/src/local/agent_conversation.rs", "acyclic-harness",
    "local::tests::agents::lost_model_producer_closes_local_session_without_settling_input",
    "settlement admitted an unrelated progress producer or performed I/O before refusal", nativeHarness,
    { mutations: [
      { patch: "rust/proofs/public-settlement-source/mutation.patch", label: "foreign-producer", diagnostic: "settlement admitted an unrelated progress producer or performed I/O before refusal" },
      { patch: "rust/proofs/public-settlement-source/cost.patch", label: "query-before-admission", diagnostic: "settlement admitted an unrelated progress producer or performed I/O before refusal" },
    ] },
  ],
  "public-settlement-retirement": [
    "rust/crates/harness/src/local/agent_conversation.rs", "acyclic-harness",
    "local::tests::agents::lost_model_producer_closes_local_session_without_settling_input",
    "public settlement retained its lost model producer", nativeHarness,
  ],
  "runtime-completion-cancellation": [
    "rust/crates/actors/src/local/runtime/completion.rs", "acyclic-actors",
    "local::runtime::tests::completion::completion_joins_discovery_delivery_and_producer_lifetime",
    "completion retained its cancelled original observer", [],
    { mutations: [
      { patch: "rust/proofs/runtime-completion-cancellation/premature.patch", label: "premature-retirement", diagnostic: "live completion retired before its original idle cut" },
      { patch: "rust/proofs/runtime-completion-cancellation/retirement.patch", label: "cancelled-observer-retained", diagnostic: "completion retained its cancelled original observer" },
      { source: "rust/crates/actors/src/local/runtime.rs", patch: "rust/proofs/runtime-completion-cancellation/dispatch.patch", label: "dispatcher-registration-omitted", diagnostic: "cancelled completion did not wake the original dispatcher" },
    ] },
  ],
  "runtime-completion-obligations": [
    "rust/crates/actors/src/local/runtime.rs", "acyclic-actors",
    "local::runtime::tests::completion::completion_joins_discovery_delivery_and_producer_lifetime",
    "completion producer survived its unpolled host", [],
    { mutations: [
      { patch: "rust/proofs/runtime-completion-custody/retirement.patch", label: "producer-loss-accepted", diagnostic: "completion producer survived its unpolled host" },
      { patch: "rust/proofs/runtime-completion-custody/cost.patch", label: "extra-control-request", diagnostic: "completion added a control request outside its original idle cut" },
    ] },
  ],
  "runtime-completion-custody": [
    "rust/crates/actors/src/local/runtime.rs", "acyclic-actors",
    "local::runtime::tests::completion::completion_joins_discovery_delivery_and_producer_lifetime",
    "completion parked without an original dispatcher producer", [],
  ],
  "retained-credit": [
    "rust/crates/filesystem/src/kernel/allocation.rs", "acyclic-fs",
    "kernel::allocation::retained::tests::retained_credit_conserves_full_width_ownership_transitions",
    "scratch retirement accepted retained credit", [],
  ],
  "settlement-terminal": [
    "rust/crates/harness/src/local/tests/agents.rs", "acyclic-harness",
    "local::tests::agents::lost_model_producer_closes_local_session_without_settling_input",
    "settlement bypassed the original runtime terminal event",
    nativeHarness,
  ],
  "native-eof-refusal": [
    "rust/crates/harness/src/local/host_process/children.rs", "acyclic-harness",
    "local::host_process::children::tools::tests::eof_retires_native_children_before_draining_a_held_reply",
    "original process must retire before reply drain",
    ["--no-default-features", "--features", "filesystem-local,native-execution"],
    { patch: "../../crates/harness/fuzz-native-journal/mutations/native_eof_refusal_retirement.patch" },
  ],
  "native-child-branch-scope": [
    "rust/crates/harness/src/local/host_workspace.rs", "acyclic-harness",
    "local::host_process::children::tests::native_reference_aliases_preserve_process_and_parent_ownership",
    "parent-scoped child reference",
    ["--no-default-features", "--features", "filesystem-local,native-execution"],
    { patch: "../../crates/harness/fuzz-native-journal/mutations/native_child_branch_scope.patch" },
  ],
  "native-child-merge-custody": [
    "rust/crates/harness/src/local/host_owner.rs", "acyclic-harness",
    "local::host_process::children::tests::native_reference_aliases_preserve_process_and_parent_ownership",
    "canonical child merge bypassed running-session custody",
    ["--no-default-features", "--features", "filesystem-local,native-execution"],
    { patch: "../../crates/harness/fuzz-native-journal/mutations/native_child_merge_custody.patch" },
  ],
  "native-added-metadata": [
    "rust/crates/filesystem/src/kernel/merge/fields.rs", "acyclic-harness",
    "local::host_process::children::tests::native_reference_aliases_preserve_process_and_parent_ownership",
    "step=codex merge right child",
    ["--no-default-features", "--features", "filesystem-local,native-execution"],
    { patch: "../../crates/filesystem/fuzz/mutations/added_metadata_observation.patch" },
  ],
  "admission-rebind": [
    "rust/crates/stream/src/session/client/admissions.rs", "acyclic-stream",
    "session::client::admissions::tests::home_rebinding_preserves_pending_memberships_without_allocating",
    "home rebinding allocated per pending membership", ["--no-default-features", ],
  ],
  "objects-empty-dispatch": [
    "rust/crates/objects/src/local.rs", "acyclic-objects",
    "local::outcome_tests::empty_ranges_preserve_native_presence_without_dispatch",
    "empty range dispatched a physical job for key", ["--no-default-features", "--features", "test-support"],
  ],
  "auth-producer-loss": [
    "rust/crates/auth/src/trust/follower.rs", "acyclic-auth",
    "trust::follower::tests::account_events_preserve_exact_dependencies_without_unrelated_wakes",
    "account wait retained its lost global producer", ["--no-default-features"],
  ],
  "auth-generation": [
    "rust/crates/auth/src/trust/follower.rs", "acyclic-auth",
    "trust::follower::tests::swap_precedes_wake_and_lost_follow_fences",
    "generation exhaustion retained a live publication owner", ["--no-default-features"],
  ],
  "trust-catchup": [
    "rust/crates/stream/src/trust_feed.rs", "acyclic-stream",
    "trust_feed::tests::losing_one_account_follow_fences_everything",
    "reconnect served trust before its original observed tail", ["--no-default-features"],
  ],
  "scalar-charge-cost": [
    "rust/crates/filesystem/src/performance.rs", "acyclic-fs",
    "performance::tests::native::borrowed::item_only_kernel_charge_preserves_exact_errors_receipts_and_cost",
    "scalar charge performed unrelated counter arithmetic", ["--no-default-features", ],
  ],
  "guest-refusal": [
    "rust/crates/actors/src/guest/state.rs", "acyclic-actors",
    "guest_loom::state::tests::lock_order_checker_rejects_same_and_different_state_nesting",
    "nested mutation lost its typed refusal", ["--features", "model-checking"],
    { target: ["--test", "main"], loom: true },
  ],
  "work-arithmetic": [
    "rust/crates/filesystem/src/performance.rs", "acyclic-fs",
    "performance::tests::full_width_work_arithmetic_preserves_exact_sums_and_refusals",
    "full-width work sum disagreed with wider arithmetic", [],
  ],
  "work-mixed": [
    "rust/crates/filesystem/src/performance.rs", "acyclic-fs",
    "performance::tests::full_width_work_arithmetic_preserves_exact_sums_and_refusals",
    "full-width work sum disagreed with wider arithmetic", [],
  ],
  "ready-source-allocation": [
    "rust/crates/stream/src/runtime/sources.rs", "acyclic-stream",
    "runtime::sources::tests::ready_records_reuse_the_registered_source_node",
    "ready record scheduling allocated physical heap storage", ["--no-default-features"],
  ],
  "budget-errors": [
    "rust/crates/filesystem/src/performance.rs", "acyclic-fs",
    "performance::tests::native::borrowed::borrowed_verification_preserves_exact_errors_and_priority",
    "budget refusal lost the exact counter or priority", [],
  ],
  "borrowed-budget": [
    "rust/crates/filesystem/src/kernel/allocation/accounting.rs", "acyclic-fs",
    "performance::tests::native::borrowed::completed_kernel_work_borrows_the_original_budget_without_allocating",
    "verification copied the original budget", [],
  ],
  "native-charge": [
    "rust/crates/filesystem/src/kernel/allocation/accounting.rs", "acyclic-fs",
    "kernel::allocation::accounting::charge_tests::native_charge_matches_the_old_path_at_every_completed_prefix",
    "kernel charge allocated physical heap storage", [],
  ],
  "controlled-cleanup": [
    "rust/crates/harness/src/local/workspace/volumes.rs", "acyclic-harness", cleanupTests[0],
    "physical cleanup escaped its original task scope", nativeHarness,
    { patch: "unscoped-claims.patch", positive: cleanupTests },
  ],
  "conversation-observation": [
    "rust/crates/harness/src/local/agent_conversation.rs", "acyclic-harness",
    "local::tests::agents::parent_and_child_context_and_writes_retain_their_own_volumes",
    "default observation repeated its settlement query", ["--all-features"],
    { oracle: "rust/crates/harness/src/local/tests/agents.rs", negativeLabel: "duplicate-settlement",
      nextest: ["--profile", "ci", "--no-fail-fast", "--failure-output", "immediate-final", "--run-ignored", "all", "--retries", "0", "--no-tests=fail"] },
  ],
  "projection-cost": [
    "rust/crates/stream/src/changes.rs",
    "acyclic-stream",
    "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding",
    "unrestricted projection must transfer the original owners",
    []
  ],
  "partial-projection-cost": [
    "rust/crates/stream/src/changes.rs",
    "acyclic-stream",
    "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding",
    "discarded change payload must not be copied",
    []
  ],
  "projection-fuzz": [
    "rust/crates/stream/src/changes.rs",
    "acyclic-stream",
    "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding",
    "projection fuzz oracle",
    [
      "--features",
      "test-support"
    ],
    {
      "patchPath": "rust/proofs/projection-fuzz/mutation.patch",
      "instrumentable": true
    }
  ],
  "default-projection-fuzz": [
    "rust/crates/stream/src/changes.rs",
    "acyclic-stream",
    "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding",
    "projection fuzz oracle",
    [],
    {
      "patchPath": "rust/proofs/projection-fuzz/mutation.patch"
    }
  ],
  "follow-digest-cost": [
    "rust/crates/stream/src/follow_filter.rs",
    "acyclic-stream",
    "follow_filter::tests::provider_hops_preserve_requested_any_keys_and_all_authority_ceilings",
    "digest scratch storage grew with repeated clauses",
    []
  ],
  "change-marker-cost": [
    "rust/crates/stream/src/changes.rs",
    "acyclic-stream",
    "changes::golden::retained_and_unknown_change_versions_keep_identity_and_one_marker_pass",
    "reserved marker classification repeated an attribute probe",
    []
  ],
  "changes-decode-cost": [
    "rust/crates/stream/src/changes.rs",
    "acyclic-stream",
    "changes::tests::admitted_decode_reserves_once_without_trusting_opaque_or_forged_keys",
    "validated decode must not allocate discarded storage",
    []
  ],
  "ready-source-race": [
    "rust/crates/stream/src/runtime/sources.rs",
    "acyclic-stream",
    "client_pool_loom::provider_source_publication_cannot_lose_its_wake",
    "provider source lost its publication wake",
    [
      "--no-default-features",
      "--features", "model-checking"
    ],
    {
      "patchPath": "rust/proofs/ready-source-parking/mutation.patch",
      "target": [
        "--test",
        "main"
      ],
      "loom": true
    }
  ],
  "children-selection": [
    "rust/crates/stream/src/lib/head.rs",
    "acyclic-stream",
    "head_tests::trusted_heads_use_the_result_budget_and_wire_heads_enforce_the_byte_bound",
    "child page escaped its original selection",
    [

    ]
  ],
  "ready-source-turn": [
    "rust/crates/stream/src/runtime/sources.rs",
    "acyclic-stream",
    "runtime::sources::tests::ready_records_reuse_the_registered_source_node",
    "ready record delivery drained unrelated provider hints",
    [
      "--no-default-features"
    ],
    {
      "mutations": [
        {
          "patch": "rust/proofs/ready-source-turn/mutation.patch",
          "diagnostic": "ready record delivery drained unrelated provider hints",
          "label": "mutated-ready-source-turn"
        },
        {
          "patch": "rust/proofs/ready-source-pending/mutation.patch",
          "diagnostic": "owner turn polled unrelated pending providers",
          "label": "mutated-ready-source-pending"
        },
        {
          "patch": "rust/proofs/ready-source-parking/mutation.patch",
          "diagnostic": "a late cancelled wake must not disarm the live provider",
          "label": "mutated-ready-source-parking"
        },
        {
          "patch": "rust/proofs/ready-source-delivery/mutation.patch",
          "diagnostic": "the live provider's committed record must be ready",
          "label": "mutated-ready-source-delivery"
        }
      ]
    }
  ],
  "ready-source-index": [
    "rust/crates/stream/src/runtime/sources.rs",
    "acyclic-stream",
    "runtime::sources::tests::ready_records_reuse_the_registered_source_node",
    "source lookup exceeded the fixed key width",
    [
      "--no-default-features"
    ]
  ],
  "retained-commit-index": [
    "rust/crates/stream/src/memory/commit_index.rs",
    "acyclic-stream",
    "memory::tests::commit_index::historical_commit_lookup_and_snapshot_rebuild_do_not_scan_retained_history",
    "retained commit lookup omitted original key examinations",
    [],
    {
      "positiveCommand": [
        "test",
        "--locked",
        "-p",
        "acyclic-stream",
        "-p",
        "acyclic-collections",
        "--lib",
        "--",
        "--exact",
        "--nocapture",
        "memory::tests::commit_index::historical_commit_lookup_and_snapshot_rebuild_do_not_scan_retained_history",
        "memory::tests::commit_index::every_commit_key_bit_fits_the_fixed_lookup_and_insertion_reservations",
        "memory::tests::commit_index::commit_index_work_is_admitted_before_append_or_coordinated_publication",
        "memory::tests::native::native_controls::prepared_replays_share_reserved_inventory_through_interleaved_growth",
        "tests::adversarial_branches_have_a_fixed_key_width_bound_and_cancel_safe_retirement",
        "tests::refused_admission_never_constructs_or_publishes_and_preserves_prior_funding"
      ],
      "completed": [
        "2 passed; 0 failed",
        "4 passed; 0 failed"
      ]
    }
  ],
  "replay-index": [
    "rust/crates/collections/src/radix.rs",
    "acyclic-stream",
    "memory::tests::replay_index::binary_replay_identity_and_work_follow_the_request_through_forks_and_recovery",
    "binary replay lookup omitted original comparison admission",
    [],
    {
      "mutations": [
        {
          "patch": "rust/proofs/replay-index/mutation.patch",
          "diagnostic": "binary replay lookup omitted original comparison admission",
          "label": "mutated"
        },
        {
          "patch": "rust/proofs/replay-index/terminal.patch",
          "diagnostic": "binary replay identity lost its original outcome",
          "label": "mutated-terminal"
        },
        {
          "patch": "rust/proofs/replay-index/boundary.patch",
          "diagnostic": "binary replay lookup escaped the requested key width",
          "label": "mutated-boundary"
        },
        {
          "patch": "rust/proofs/replay-index/credit.patch",
          "diagnostic": "reserved index examinations",
          "label": "mutated-credit",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-stream",
            "--lib",
            "memory::tests::native::native_controls::prepared_replays_share_reserved_inventory_through_interleaved_growth",
            "--",
            "--exact",
            "--nocapture"
          ]
        },
        {
          "patch": "rust/proofs/replay-index/recovery.patch",
          "diagnostic": "reserved index examinations",
          "label": "mutated-recovery",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-stream",
            "--lib",
            "memory::tests::native::native_controls::prepared_replays_share_reserved_inventory_through_interleaved_growth",
            "--",
            "--exact",
            "--nocapture"
          ],
          "source": "rust/crates/stream/src/memory/prepared.rs"
        }
      ],
      "positiveCommand": [
        "test",
        "--locked",
        "-p",
        "acyclic-stream",
        "-p",
        "acyclic-collections",
        "--lib",
        "--",
        "--exact",
        "--nocapture",
        "memory::tests::replay_index::binary_replay_identity_and_work_follow_the_request_through_forks_and_recovery",
        "memory::tests::native::native_controls::prepared_replays_share_reserved_inventory_through_interleaved_growth",
        "memory::tests::native::native_controls::descending_replay_keys_keep_copy_work_and_lookup_depth_bounded",
        "memory::tests::native::native_controls::prepared_replay_probe_credit_is_required_before_acceptance",
        "memory::tests::commit_index::every_commit_key_bit_fits_the_fixed_lookup_and_insertion_reservations",
        "tests::adversarial_branches_have_a_fixed_key_width_bound_and_cancel_safe_retirement",
        "tests::refused_admission_never_constructs_or_publishes_and_preserves_prior_funding"
      ],
      "completed": [
        "2 passed; 0 failed",
        "5 passed; 0 failed"
      ]
    }
  ],
  "native-custody": [
    "rust/crates/native-runtime/src/task.rs",
    "acyclic-native-runtime",
    "task::custody::tests::original_tasks_conform_to_scoped_and_shared_completion_custody",
    "shared completion replaced the original operation error",
    [],
    {
      "mutations": [
        {
          "source": "rust/crates/native-runtime/src/task/custody.rs",
          "patch": "rust/proofs/shared-completion-error/mutation.patch",
          "diagnostic": "shared completion replaced the original operation error",
          "label": "mutated-shared-completion-error",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-native-runtime",
            "--lib",
            "task::custody::tests::original_tasks_conform_to_scoped_and_shared_completion_custody",
            "--",
            "--exact",
            "--nocapture"
          ]
        },
        {
          "source": "rust/crates/native-runtime/src/task/custody.rs",
          "patch": "rust/proofs/task-cancellation-cause/mutation.patch",
          "diagnostic": "task cancellation lost its original typed cause",
          "label": "mutated-task-cancellation-cause",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-native-runtime",
            "--lib",
            "task::custody::tests::original_tasks_conform_to_scoped_and_shared_completion_custody",
            "--",
            "--exact",
            "--nocapture"
          ]
        },
        {
          "source": "rust/crates/native-runtime/src/task/custody.rs",
          "patch": "rust/proofs/shared-completion-cause/mutation.patch",
          "diagnostic": "shared completion replaced the original typed cause",
          "label": "mutated-shared-completion-cause",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-native-runtime",
            "--lib",
            "task::custody::tests::original_tasks_conform_to_scoped_and_shared_completion_custody",
            "--",
            "--exact",
            "--nocapture"
          ]
        },
        {
          "source": "rust/crates/objects/src/local.rs",
          "patch": "rust/proofs/objects-close-result/mutation.patch",
          "diagnostic": "close hid",
          "label": "mutated-objects-close-result",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-objects",
            "--no-default-features",
            "--features", "test-support",
            "--lib",
            "local::outcome_tests::owned_close_reports_physical_completion_and_failure",
            "--",
            "--exact",
            "--nocapture"
          ]
        },
        {
          "source": "rust/crates/native-runtime/src/task/custody.rs",
          "patch": "rust/proofs/persistence-panic/mutation.patch",
          "diagnostic": "left: Interrupted",
          "label": "mutated-persistence-panic",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-native-runtime",
            "--no-default-features",
            "--lib",
            "task::tests::persistence_worker_panic_terminates_its_original_wait",
            "--",
            "--exact",
            "--nocapture"
          ]
        },
        {
          "source": "rust/crates/native-runtime/src/task.rs",
          "patch": "rust/proofs/persistence-wait-retirement/mutation.patch",
          "diagnostic": "a failed producer retained its wait",
          "label": "mutated-persistence-wait-retirement",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-native-runtime",
            "--no-default-features",
            "--lib",
            "task::tests::persistence_worker_panic_terminates_its_original_wait",
            "--",
            "--exact",
            "--nocapture"
          ]
        },
        {
          "source": "rust/crates/native-runtime/src/task.rs",
          "patch": "rust/proofs/persistence-caller-custody/mutation.patch",
          "diagnostic": "caller drop abandoned the original persistence body",
          "label": "mutated-persistence-caller-custody",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-native-runtime",
            "--no-default-features",
            "--lib",
            "task::tests::persistence_retains_accepted_work_after_caller_drop",
            "--",
            "--exact",
            "--nocapture"
          ]
        },
        {
          "source": "rust/crates/native-runtime/src/task.rs",
          "patch": "rust/proofs/persistence-reentry/mutation.patch",
          "diagnostic": "Future::poll(accepted.as_mut(), &mut context)",
          "label": "mutated-persistence-reentry",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-native-runtime",
            "--no-default-features",
            "--lib",
            "task::tests::persistence_reports_the_original_job_and_refuses_same_service_dependencies",
            "--",
            "--exact",
            "--nocapture"
          ]
        },
        {
          "source": "rust/crates/native-runtime/src/task.rs",
          "patch": "rust/proofs/persistence-submission/mutation.patch",
          "diagnostic": "reentrant persistence submission retained or ran its body",
          "label": "mutated-persistence-submission",
          "command": [
            "test",
            "--locked",
            "-p",
            "acyclic-native-runtime",
            "--no-default-features",
            "--lib",
            "task::tests::persistence_reports_the_original_job_and_refuses_same_service_dependencies",
            "--",
            "--exact",
            "--nocapture"
          ]
        }
      ],
      "positiveCommand": [
        "test",
        "--locked",
        "-p",
        "acyclic-native-runtime",
        "-p",
        "acyclic-objects",
        "--features", "acyclic-objects/test-support",
        "--lib",
        "--",
        "--exact",
        "--nocapture",
        "task::custody::tests::original_tasks_conform_to_scoped_and_shared_completion_custody",
        "task::custody::tests::seeded_executor_uses_the_same_scoped_custody_contract",
        "task::tests::persistence_reports_the_original_job_and_refuses_same_service_dependencies",
        "task::tests::persistence_worker_panic_terminates_its_original_wait",
        "task::tests::persistence_retains_accepted_work_after_caller_drop",
        "local::outcome_tests::owned_close_reports_physical_completion_and_failure"
      ],
      "completed": [
        "5 passed; 0 failed",
        "1 passed; 0 failed"
      ]
    }
  ],
};
const [claim, ...extra] = process.argv.slice(2);
const instrumented = extra.length === 1 && extra[0] === "--instrumented" && claims[claim]?.[5]?.instrumentable;
const from = extra.length === 2 && extra[0] === "--from" ? extra[1] : undefined;
const help = `node rust/proofs/qualify.mjs <${Object.keys(claims).join("|")}> [--from mutation-label]`;
if (claim === "--help" && extra.length === 0) {
  console.log(help);
} else if (!Object.hasOwn(claims, claim) || (extra.length !== 0 && !instrumented && from === undefined)) {
  console.error(`step=select claim; cause=expected exactly one supported claim; recovery: ${help}`);
  process.exitCode = 2;
} else {
  const [source, crate, test, diagnostic, features, options = {}] = claims[claim];
  const environment = { ...process.env, ...options.env };
  const tests = [test, ...(options.additionalTests ?? [])];
  const sourcePath = new URL(source, new URL("../../", import.meta.url));
  const patchRelative = claim === "work-mixed"
    ? "rust/proofs/work-arithmetic/mixed-mutation.patch"
    : options.patchPath ?? `rust/proofs/${claim}/${options.patch ?? "mutation.patch"}`;
  const patch = fileURLToPath(new URL(patchRelative, new URL("../../", import.meta.url)));
  const target = options.target ?? ["--lib"];
  let fuzzTarget;
  if (options.fuzz) {
    const compiler = spawnSync("rustc", ["-vV"], { encoding: "utf8" });
    fuzzTarget = compiler.stdout?.match(/^host: (\S+)$/m)?.[1];
    if (compiler.error || compiler.status !== 0 || !fuzzTarget) {
      console.error(`step=select native fuzz target; cause=${compiler.error?.message ?? compiler.stderr ?? "rustc host absent"}; recovery: rustc -vV; then node rust/proofs/qualify.mjs ${claim}`);
      process.exit(2);
    }
  }
  const fuzzCommand = runs => ["run", "--locked", "-p", crate, ...features, "--bin", options.fuzz.bin,
    "--target", fuzzTarget,
    "--", options.fuzz.corpus, `-runs=${runs}`, "-seed=71", "-print_final_stats=1",
    `-artifact_prefix=target/${claim}/`];
  const command = instrumented
    ? ["+nightly", "fuzz", "run", "--fuzz-dir", ".", "stream_persistence", "corpus/stream_persistence/projection-v1-append", "--", "-runs=1000", "-seed=1697"]
    : options.command ?? (options.fuzz ? fuzzCommand(1) : options.nextest
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E", tests.map(name => `test(=${name})`).join(" | "), ...options.nextest]
    : ["test", "--locked", "-p", crate, ...features, ...target, "--", "--exact", "--nocapture", ...tests]);
  const positive = instrumented
    ? ["+nightly", "fuzz", "run", "--fuzz-dir", ".", "stream_persistence", fileURLToPath(new URL("target/projection-fuzz/corpus", new URL("../../", import.meta.url))), "corpus/stream_persistence", "--", "-runs=1000", "-seed=1697"]
    : options.positiveCommand ?? (options.fuzz ? fuzzCommand(options.fuzz.runs) : options.positive
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E",
      options.positive.map(name => `test(=${name})`).join(" | "), ...(options.nextest ?? ["--no-tests=fail"])]
    : command);
  const sources = [...new Set([source, ...(options.mutations ?? []).map(mutation => mutation.source ?? source)])];
  const recovery = `git diff -- ${sources.join(" ")}; inspect target/${claim}/*.log; correct source or command, then node rust/proofs/qualify.mjs ${claim}${instrumented ? " --instrumented" : from === undefined ? "" : ` --from ${from}`}`;
  const directory = new URL(`target/${claim}/`, new URL("../../", import.meta.url));
  const oraclePath = options.oracle && new URL(options.oracle, new URL("../../", import.meta.url));
  let oracle;
  const phases = [];
  let step = "read original production source";
  function require(condition) {
    if (!condition) throw new Error("required qualification outcome absent");
  }
  function run(program, args, log) {
    const descriptor = log && openSync(log, "w");
    let result;
    try {
      result = spawnSync(program, args, {
        cwd: instrumented && program === "cargo" ? fileURLToPath(new URL("rust/codec-fuzz/", new URL("../../", import.meta.url))) : root, encoding: "utf8",
        ...(log ? { stdio: ["ignore", descriptor, descriptor] } : {}),
        env: {
          ...environment, PROPTEST_RNG_SEED: environment.PROPTEST_RNG_SEED ?? "1697",
          ...(options.fuzz ? {
            // RUSTFLAGS replaces Cargo configuration, including replay's Tokio support.
            RUSTFLAGS: `${environment.RUSTFLAGS?.includes("-Cpasses=sancov-module") ? environment.RUSTFLAGS
              : `${environment.RUSTFLAGS ?? ""} -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 -Cllvm-args=-sanitizer-coverage-inline-8bit-counters -Cllvm-args=-sanitizer-coverage-pc-table -Cllvm-args=-sanitizer-coverage-trace-compares`} --cfg tokio_unstable`,
          } : {}),
          ...(options.loom ? {
            RUSTC_WORKSPACE_WRAPPER: fileURLToPath(new URL(
              `../../scripts/loom-rustc.${process.platform === "win32" ? "cmd" : "sh"}`, import.meta.url)),
          } : {}),
        },
      });
    } finally {
      if (descriptor !== undefined) closeSync(descriptor);
    }
    if (result.error) throw result.error;
    return { ...result, output: log ? readFileSync(log, "utf8") : `${result.stdout ?? ""}${result.stderr ?? ""}` };
  }
  function check(label, selected = command, checkedSource = source, expectedOracle = oracle) {
    if (oracle) require(readFileSync(oraclePath).equals(expectedOracle));
    mkdirSync(directory, { recursive: true });
    const invocation = options.runner ?? ["cargo", ...selected];
    const result = run(invocation[0], invocation.slice(1), new URL(`${label}.log`, directory));
    console.log(`step=${label}; command=${JSON.stringify(invocation)}; exit=${result.status}; signal=${result.signal}`);
    const lines = result.output.trimEnd().split(/\r?\n/);
    const firstError = lines.findIndex(line => /^error(?:\[|:)/.test(line));
    if (firstError >= 0 && firstError < lines.length - 35) {
      console.log(lines.slice(firstError, firstError + 12).join("\n"));
    }
    console.log(lines.slice(-35).join("\n"));
    if (options.fuzz) {
      const coverage = result.output.match(/^#.*\bcov:.*$/gm)?.at(-1);
      if (coverage) console.log(coverage);
    }
    if (oracle) {
      const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
      const checkedPath = new URL(checkedSource, new URL("../../", import.meta.url));
      const phase = { phase: label, exit: result.status, source: checkedSource, source_sha256: sha256(readFileSync(checkedPath)), oracle_sha256: sha256(expectedOracle) };
      phases.push(phase);
      console.log(JSON.stringify(phase));
      require(readFileSync(oraclePath).equals(expectedOracle));
      const count = label === "restored" ? options.positiveCount ?? options.positive?.length ?? tests.length : tests.length;
      const started = options.invoked ?? (invocation[0] === "cargo" && invocation[1] === "nextest"
        ? `Starting ${count} ${count === 1 ? "test" : "tests"} across 1 binary`
        : `running ${count} ${count === 1 ? "test" : "tests"}`);
      require(result.output.includes(started));
    }
    return result;
  }
  try {
    if (oraclePath) oracle = readFileSync(oraclePath);
    if (instrumented) mkdirSync(new URL("corpus/", directory), { recursive: true });
    const mutations = options.mutations ?? [{ patch, diagnostic, label: options.negativeLabel ?? "mutated" }];
    step = "select first mutation";
    const start = from === undefined ? 0 : mutations.findIndex(mutation => mutation.label === from);
    if (start < 0) throw new Error(`unknown mutation label ${JSON.stringify(from)}`);
    if (start !== 0) console.log(`selection=partial; preceding=${start} mutations require their own source-bound receipts`);
    for (const mutation of mutations.slice(start)) {
      const mutatedPath = mutation.source ? new URL(mutation.source, new URL("../../", import.meta.url)) : sourcePath;
      const original = readFileSync(mutatedPath);
      step = `check mutation anchors: ${mutation.patch}`;
      const anchor = run("git", ["apply", "--check", mutation.patch]);
      if (anchor.status !== 0) throw new Error(anchor.output);
      try {
        step = `apply production mutation: ${mutation.patch}`;
        const applied = run("git", ["apply", mutation.patch]);
        if (applied.status !== 0) throw new Error(applied.output);
        if (mutation.priorOraclePatch) {
          require(oracle !== undefined);
          try {
            step = `apply prior oracle: ${mutation.priorOraclePatch}`;
            const prior = run("git", ["apply", mutation.priorOraclePatch]);
            if (prior.status !== 0) throw new Error(prior.output);
            step = "prior oracle misses the same production mutation";
            const missed = check(`${mutation.label}-prior`, mutation.command ?? command,
              mutation.source ?? source, readFileSync(oraclePath));
            require(missed.status === 0 && !missed.signal
              && options.completed.every(marker => missed.output.includes(marker)));
          } finally {
            writeFileSync(oraclePath, oracle);
          }
        }
        step = `unchanged oracle rejects production mutation: ${mutation.patch}`;
        const mutated = check(mutation.label, mutation.command ?? command, mutation.source ?? source);
        const diagnostics = [mutation.diagnostic, ...(mutation.additionalDiagnostics ?? options.additionalDiagnostics ?? [])];
        require(instrumented
          ? mutated.status !== 0 && !mutated.signal && mutated.output.includes("assertion failed: acyclic_stream::codec_fuzz::check(bytes)") && mutated.output.includes("ERROR: libFuzzer:")
          : options.fuzz ? mutated.status !== 0 && !mutated.signal && diagnostics.every(message => mutated.output.includes(message)) && mutated.output.includes("ERROR: libFuzzer:")
          : mutated.status === (options.negativeExit ?? (options.nextest ? 100 : 101)) && !mutated.signal && diagnostics.every(message => mutated.output.includes(message))
            && (options.started === undefined || mutated.output.includes(options.started))
            && (options.nextest || mutated.output.includes(options.failed === true ? mutation.diagnostic : options.failed ?? `0 passed; ${tests.length} failed`)));
      } finally {
        writeFileSync(mutatedPath, original);
      }
      step = "restore exact original source";
      require(readFileSync(mutatedPath).equals(original));
    }
    step = "restored original oracle completes";
    const restored = check("restored", positive);
    const completed = options.completed ?? (options.fuzz ? ["DONE", `${options.fuzz.runs} runs`, "cov:"] : [options.positive ? `${options.positive.length} tests run: ${options.positive.length} passed`
      : options.nextest ? `${tests.length} test${tests.length === 1 ? "" : "s"} run: ${tests.length} passed` : `${tests.length} passed; 0 failed`]);
    require(restored.status === 0 && !restored.signal && (instrumented
      ? ["DONE", "1000 runs", "cov:"].every(marker => restored.output.includes(marker))
      : completed.every(marker => restored.output.includes(marker))));
  } catch (error) {
    console.error(`step=${step}; cause=${error.message}; recovery: ${recovery}`);
    process.exitCode = 1;
  } finally {
    if (oracle) {
      try {
        mkdirSync(directory, { recursive: true });
        writeFileSync(new URL("phases.json", directory), `${JSON.stringify(phases, null, 2)}\n`);
      } catch (error) {
        console.error(`step=write phase report; cause=${error.message}; recovery: ${recovery}`);
        process.exitCode = 1;
      }
    }
  }
}
