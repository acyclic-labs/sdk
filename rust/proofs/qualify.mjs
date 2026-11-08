#!/usr/bin/env node
// Qualify a production oracle and its retained mutation.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { closeSync, mkdirSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
const nativeHarness = ["--features", "filesystem-local,native-execution,test-support,grpc,search-http"];
const cleanupTests = [
  "local::workspace::volumes::publication_tests::cancelled_command_retires_before_next_entrant_and_preserves_acknowledged_head",
  "local::tests::agents::a_shell_call_stages_its_output_on_the_private_volume",
  "local::owner::publication_tests::private_legacy_owner_migrates_once_without_changing_its_original_identity",
  "local::owner::publication_tests::concurrent_owner_migration_cannot_publish_another_callers_provisional_key",
  "local::owner::publication_tests::explicit_file_custody_reopens_the_original_owner_without_native_fallback",
];
const claims = {
  "cli-failure-output": [
    "rust/crates/harness/src/completion.rs", "graphcoder",
    "lifecycle::tests::failure_output_retains_cause_and_progress_with_native_backpressure",
    "failure output retained a lost shutdown producer", [],
    { mutations: [
      { patch: "rust/proofs/cli-failure-output/shutdown.patch", label: "shutdown-omitted", diagnostic: "failure output retained a lost shutdown producer" },
    ] },
  ],
  "native-hard-link-detached-facts": [
    "rust/crates/filesystem/src/native_mount/lazy/mutations.rs", "acyclic-fs",
    "native_mount::lazy::tests::publication::source_hard_link_open_unlink_write_preserves_surviving_alias",
    "hard-link reply discarded current detached file facts",
    ["--no-default-features", "--features", "memory,native-mount"],
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
  "trust-fenced-close-progress": [
    "rust/crates/stream/src/trust_feed.rs", "acyclic-stream",
    "trust_anchor::closing_a_fenced_environment_ends_its_original_anchor_wait",
    "closed follower left its original anchor wait parked", ["--features", "test-support"],
    { target: ["--test", "main"], patchPath: "rust/proofs/trust-fenced-close-progress/mutation.patch", negativeLabel: "omitted-original-follower-close-observation" },
  ],
  "local-ready-snapshot-allocation": [
    "rust/crates/stream/src/local/local_stream.rs", "acyclic-stream",
    "local::snapshot_ready::ready_snapshots_allocate_zero_and_match_independent_prefixes",
    "ready snapshots must allocate zero times", ["--no-default-features", "--features", "local,test-support"],
    {
      mutations: [
        { patch: "rust/proofs/local-ready-snapshot-allocation/mutation.patch", label: "boxed-original-local-snapshot", diagnostic: "ready snapshots must allocate zero times" },
        { patch: "rust/proofs/local-failure-progress/observation.patch", label: "missing-observation-retirement", diagnostic: "failed journal left a public observation parked",
          command: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--features", "local,test-support", "--lib", "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers", "--", "--exact", "--nocapture"] },
      ],
      positiveCommand: ["test", "--locked", "-p", "acyclic-stream", "--no-default-features", "--features", "local,test-support", "--lib", "--", "--exact", "--nocapture",
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
    "abandoned prepared producer did not reach its original completion boundary", ["--no-default-features", "--features", "local"],
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
    "retired prepared producer left its original completion parked or replaced its cause", ["--no-default-features", "--features", "local"],
    {
      patchPath: "rust/proofs/local-prepared-producer-loss/mutation.patch",
      negativeLabel: "discarded-original-prepared-producer-error",
    },
  ],
  "local-snapshot-producer-loss": [
    "rust/crates/stream/src/snapshot.rs", "acyclic-stream",
    "local::tests::wakeups::executor_retirement_ends_forked_follows_before_publication_unlocks",
    "retired local snapshot left its public follow parked", ["--no-default-features", "--features", "local"],
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
    "failed journal left a future revision parked", ["--features", "local,test-support"],
    { patch: "retirement.patch" },
  ],
  "local-failure-progress": [
    "rust/crates/stream/src/local/local_stream.rs", "acyclic-stream",
    "local::tests::failure::failed_physical_write_keeps_its_cause_and_ends_followers",
    "failed journal left a public observation parked", ["--features", "local,test-support"],
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
    "failed journal left a native guarded commit parked", ["--features", "local,test-support"],
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
    "retired refresh producer left a public revision wait parked", ["--features", "postgres"],
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
  "admission-rebind": [
    "rust/crates/stream/src/session/client/admissions.rs", "acyclic-stream",
    "session::client::admissions::tests::home_rebinding_preserves_pending_memberships_without_allocating",
    "home rebinding allocated per pending membership", ["--no-default-features", "--features", "session"],
  ],
  "objects-empty-dispatch": [
    "rust/crates/objects/src/local.rs", "acyclic-objects",
    "local::outcome_tests::empty_ranges_preserve_native_presence_without_dispatch",
    "empty range dispatched a physical job for key", ["--no-default-features", "--features", "local,test-support"],
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
    "scalar charge performed unrelated counter arithmetic", ["--no-default-features", "--features", "distributed"],
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
      "--features",
      "grpc,model-checking"
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
      "--features",
      "local"
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
            "--features",
            "local,test-support",
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
        "--features",
        "acyclic-objects/local,acyclic-objects/test-support",
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
  const sourcePath = new URL(source, new URL("../../", import.meta.url));
  const patch = claim === "work-mixed"
    ? "rust/proofs/work-arithmetic/mixed-mutation.patch"
    : options.patchPath ?? `rust/proofs/${claim}/${options.patch ?? "mutation.patch"}`;
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
  const fuzzCommand = runs => ["run", "--locked", "-p", crate, "--bin", options.fuzz.bin,
    "--target", fuzzTarget,
    "--", options.fuzz.corpus, `-runs=${runs}`, "-seed=71", "-print_final_stats=1",
    `-artifact_prefix=target/${claim}/`];
  const command = instrumented
    ? ["+nightly", "fuzz", "run", "--fuzz-dir", ".", "stream_persistence", "corpus/stream_persistence/projection-v1-append", "--", "-runs=1000", "-seed=1697"]
    : options.fuzz ? fuzzCommand(1) : options.nextest
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E", `test(=${test})`, ...options.nextest]
    : ["test", "--locked", "-p", crate, ...features, ...target, test, "--", "--exact", "--nocapture"];
  const positive = instrumented
    ? ["+nightly", "fuzz", "run", "--fuzz-dir", ".", "stream_persistence", fileURLToPath(new URL("target/projection-fuzz/corpus", new URL("../../", import.meta.url))), "corpus/stream_persistence", "--", "-runs=1000", "-seed=1697"]
    : options.positiveCommand ?? (options.fuzz ? fuzzCommand(options.fuzz.runs) : options.positive
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E",
      options.positive.map(name => `test(=${name})`).join(" | "), "--no-tests=fail"]
    : command);
  const recovery = `git diff -- ${source}; inspect target/${claim}/*.log; correct source or command, then node rust/proofs/qualify.mjs ${claim}${instrumented ? " --instrumented" : from === undefined ? "" : ` --from ${from}`}`;
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
          ...process.env, PROPTEST_RNG_SEED: process.env.PROPTEST_RNG_SEED ?? "1697",
          ...(options.fuzz ? {
            RUSTFLAGS: process.env.RUSTFLAGS?.includes("-Cpasses=sancov-module") ? process.env.RUSTFLAGS
              : `${process.env.RUSTFLAGS ?? ""} -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 -Cllvm-args=-sanitizer-coverage-inline-8bit-counters -Cllvm-args=-sanitizer-coverage-pc-table -Cllvm-args=-sanitizer-coverage-trace-compares`,
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
  function check(label, selected = command) {
    if (oracle) require(readFileSync(oraclePath).equals(oracle));
    mkdirSync(directory, { recursive: true });
    const invocation = options.runner ?? ["cargo", ...selected];
    const result = run(invocation[0], invocation.slice(1), new URL(`${label}.log`, directory));
    if (oracle) {
      const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
      const phase = { phase: label, exit: result.status, source_sha256: sha256(readFileSync(sourcePath)), oracle_sha256: sha256(oracle) };
      phases.push(phase);
      console.log(JSON.stringify(phase));
      require(readFileSync(oraclePath).equals(oracle));
      require(result.output.includes("Starting 1 test across 1 binary"));
    }
    console.log(`step=${label}; command=${JSON.stringify(invocation)}; exit=${result.status}; signal=${result.signal}`);
    console.log(result.output.trimEnd().split(/\r?\n/).slice(-35).join("\n"));
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
        step = `unchanged oracle rejects production mutation: ${mutation.patch}`;
        const mutated = check(mutation.label, mutation.command ?? command);
        require(instrumented
          ? mutated.status !== 0 && !mutated.signal && mutated.output.includes("assertion failed: acyclic_stream::codec_fuzz::check(bytes)") && mutated.output.includes("ERROR: libFuzzer:")
          : options.fuzz ? mutated.status !== 0 && !mutated.signal && mutated.output.includes(mutation.diagnostic) && mutated.output.includes("ERROR: libFuzzer:")
          : mutated.status === (options.negativeExit ?? (options.nextest ? 100 : 101)) && !mutated.signal && mutated.output.includes(mutation.diagnostic)
            && (options.started === undefined || mutated.output.includes(options.started))
            && (options.nextest || mutated.output.includes(options.failed === true ? mutation.diagnostic : options.failed ?? "0 passed; 1 failed")));
      } finally {
        writeFileSync(mutatedPath, original);
      }
      step = "restore exact original source";
      require(readFileSync(mutatedPath).equals(original));
    }
    step = "restored original oracle completes";
    const restored = check("restored", positive);
    const completed = options.completed ?? (options.fuzz ? ["DONE", `${options.fuzz.runs} runs`, "cov:"] : [options.positive ? `${options.positive.length} tests run: ${options.positive.length} passed`
      : options.nextest ? "1 test run: 1 passed" : "1 passed; 0 failed"]);
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
