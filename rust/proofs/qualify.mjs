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
  "projection-cost": ["rust/crates/stream/src/changes.rs", "acyclic-stream", "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding", "unrestricted projection must transfer the original owners", []],
  "partial-projection-cost": ["rust/crates/stream/src/changes.rs", "acyclic-stream", "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding", "discarded change payload must not be copied", []],
  "projection-fuzz": ["rust/crates/stream/src/changes.rs", "acyclic-stream", "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding", "projection fuzz oracle", ["--features", "test-support"], {"patchPath": "rust/proofs/projection-fuzz/mutation.patch", "instrumentable": true}],
  "default-projection-fuzz": ["rust/crates/stream/src/changes.rs", "acyclic-stream", "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding", "projection fuzz oracle", [], {"patchPath": "rust/proofs/projection-fuzz/mutation.patch"}],
  "follow-digest-cost": ["rust/crates/stream/src/follow_filter.rs", "acyclic-stream", "follow_filter::tests::provider_hops_preserve_requested_any_keys_and_all_authority_ceilings", "digest scratch storage grew with repeated clauses", []],
  "change-marker-cost": ["rust/crates/stream/src/changes.rs", "acyclic-stream", "changes::golden::retained_and_unknown_change_versions_keep_identity_and_one_marker_pass", "reserved marker classification repeated an attribute probe", []],
  "changes-decode-cost": ["rust/crates/stream/src/changes.rs", "acyclic-stream", "changes::tests::admitted_decode_reserves_once_without_trusting_opaque_or_forged_keys", "validated decode must not allocate discarded storage", []],
  "ready-source-race": ["rust/crates/stream/src/runtime/sources.rs", "acyclic-stream", "client_pool_loom::provider_source_publication_cannot_lose_its_wake", "provider source lost its publication wake", ["--no-default-features", "--features", "grpc,model-checking"], {"patchPath": "rust/proofs/ready-source-parking/mutation.patch", "target": ["--test", "main"], "loom": true}],
  "children-selection": ["rust/crates/stream/src/lib/head.rs", "acyclic-stream", "head_tests::trusted_heads_use_the_result_budget_and_wire_heads_enforce_the_byte_bound", "child page escaped its original selection", ["--features", "local"]],
  "ready-source-turn": ["rust/crates/stream/src/runtime/sources.rs", "acyclic-stream", "runtime::sources::tests::ready_records_reuse_the_registered_source_node", "ready record delivery drained unrelated provider hints", ["--no-default-features"], {"mutations": [{"patch": "rust/proofs/ready-source-turn/mutation.patch", "diagnostic": "ready record delivery drained unrelated provider hints", "label": "mutated-ready-source-turn"}, {"patch": "rust/proofs/ready-source-pending/mutation.patch", "diagnostic": "owner turn polled unrelated pending providers", "label": "mutated-ready-source-pending"}, {"patch": "rust/proofs/ready-source-parking/mutation.patch", "diagnostic": "a late cancelled wake must not disarm the live provider", "label": "mutated-ready-source-parking"}, {"patch": "rust/proofs/ready-source-delivery/mutation.patch", "diagnostic": "the live provider's committed record must be ready", "label": "mutated-ready-source-delivery"}]}],
  "ready-source-index": ["rust/crates/stream/src/runtime/sources.rs", "acyclic-stream", "runtime::sources::tests::ready_records_reuse_the_registered_source_node", "source lookup exceeded the fixed key width", ["--no-default-features"]],
  "retained-commit-index": ["rust/crates/stream/src/memory/commit_index.rs", "acyclic-stream", "memory::tests::commit_index::historical_commit_lookup_and_snapshot_rebuild_do_not_scan_retained_history", "retained commit lookup omitted original key examinations", [], {"positiveCommand": ["test", "--locked", "-p", "acyclic-stream", "-p", "acyclic-collections", "--lib", "--", "--exact", "--nocapture", "memory::tests::commit_index::historical_commit_lookup_and_snapshot_rebuild_do_not_scan_retained_history", "memory::tests::commit_index::every_commit_key_bit_fits_the_fixed_lookup_and_insertion_reservations", "memory::tests::commit_index::commit_index_work_is_admitted_before_append_or_coordinated_publication", "memory::tests::native::native_controls::prepared_replays_share_reserved_inventory_through_interleaved_growth", "tests::adversarial_branches_have_a_fixed_key_width_bound_and_cancel_safe_retirement", "tests::refused_admission_never_constructs_or_publishes_and_preserves_prior_funding"], "completed": ["2 passed; 0 failed", "4 passed; 0 failed"]}],
};
const [claim, ...extra] = process.argv.slice(2);
const instrumented = extra.length === 1 && extra[0] === "--instrumented" && claims[claim]?.[5]?.instrumentable;
const help = `node rust/proofs/qualify.mjs <${Object.keys(claims).join("|")}>`;
if (claim === "--help" && extra.length === 0) {
  console.log(help);
} else if (!Object.hasOwn(claims, claim) || (extra.length !== 0 && !instrumented)) {
  console.error(`step=select claim; cause=expected exactly one supported claim; recovery: ${help}`);
  process.exitCode = 2;
} else {
  const [source, crate, test, diagnostic, features, options = {}] = claims[claim];
  const sourcePath = new URL(source, new URL("../../", import.meta.url));
  const patch = claim === "work-mixed"
    ? "rust/proofs/work-arithmetic/mixed-mutation.patch"
    : options.patchPath ?? `rust/proofs/${claim}/${options.patch ?? "mutation.patch"}`;
  const target = options.target ?? ["--lib"];
  const command = instrumented
    ? ["+nightly", "fuzz", "run", "--fuzz-dir", ".", "stream_persistence", "corpus/stream_persistence/projection-v1-append", "--", "-runs=1000", "-seed=1697"]
    : options.nextest
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E", `test(=${test})`, ...options.nextest]
    : ["test", "--locked", "-p", crate, ...features, ...target, test, "--", "--exact", "--nocapture"];
  const positive = instrumented
    ? ["+nightly", "fuzz", "run", "--fuzz-dir", ".", "stream_persistence", fileURLToPath(new URL("target/projection-fuzz/corpus", new URL("../../", import.meta.url))), "corpus/stream_persistence", "--", "-runs=1000", "-seed=1697"]
    : options.positiveCommand ?? (options.positive
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E",
      options.positive.map(name => `test(=${name})`).join(" | "), "--no-tests=fail"]
    : command);
  const recovery = `git diff -- ${source}; inspect target/${claim}/*.log; correct source or command, then node rust/proofs/qualify.mjs ${claim}${instrumented ? " --instrumented" : ""}`;
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
    const result = run("cargo", selected, new URL(`${label}.log`, directory));
    if (oracle) {
      const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
      const phase = { phase: label, exit: result.status, source_sha256: sha256(readFileSync(sourcePath)), oracle_sha256: sha256(oracle) };
      phases.push(phase);
      console.log(JSON.stringify(phase));
      require(readFileSync(oraclePath).equals(oracle));
      require(result.output.includes("Starting 1 test across 1 binary"));
    }
    console.log(`step=${label}; command=${JSON.stringify(["cargo", ...selected])}; exit=${result.status}; signal=${result.signal}`);
    console.log(result.output.trimEnd().split(/\r?\n/).slice(-35).join("\n"));
    return result;
  }
  try {
    if (oraclePath) oracle = readFileSync(oraclePath);
    if (instrumented) mkdirSync(new URL("corpus/", directory), { recursive: true });
    const mutations = options.mutations ?? [{ patch, diagnostic, label: options.negativeLabel ?? "mutated" }];
    for (const mutation of mutations) {
      const original = readFileSync(sourcePath);
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
          : mutated.status === (options.nextest ? 100 : 101) && !mutated.signal && mutated.output.includes(mutation.diagnostic)
            && (options.nextest || mutated.output.includes("0 passed; 1 failed")));
      } finally {
        writeFileSync(sourcePath, original);
      }
      step = "restore exact original source";
      require(readFileSync(sourcePath).equals(original));
    }
    step = "restored original oracle completes";
    const restored = check("restored", positive);
    const completed = options.completed ?? [options.positive ? `${options.positive.length} tests run: ${options.positive.length} passed`
      : options.nextest ? "1 test run: 1 passed" : "1 passed; 0 failed"];
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
