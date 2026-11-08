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
};
const [claim, ...extra] = process.argv.slice(2);
const help = `node rust/proofs/qualify.mjs <${Object.keys(claims).join("|")}>`;
if (claim === "--help" && extra.length === 0) {
  console.log(help);
} else if (!Object.hasOwn(claims, claim) || extra.length !== 0) {
  console.error(`step=select claim; cause=expected exactly one supported claim; recovery: ${help}`);
  process.exitCode = 2;
} else {
  const [source, crate, test, diagnostic, features, options = {}] = claims[claim];
  const sourcePath = new URL(source, new URL("../../", import.meta.url));
  const patch = claim === "work-mixed"
    ? "rust/proofs/work-arithmetic/mixed-mutation.patch"
    : `rust/proofs/${claim}/${options.patch ?? "mutation.patch"}`;
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
  const command = options.fuzz ? fuzzCommand(1) : options.nextest
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E", `test(=${test})`, ...options.nextest]
    : ["test", "--locked", "-p", crate, ...features, ...target, test, "--", "--exact", "--nocapture"];
  const positive = options.positiveCommand ?? (options.fuzz ? fuzzCommand(options.fuzz.runs) : options.positive
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E",
      options.positive.map(name => `test(=${name})`).join(" | "), "--no-tests=fail"]
    : command);
  const recovery = `git diff -- ${source}; inspect target/${claim}/*.log; correct source or command, then node rust/proofs/qualify.mjs ${claim}`;
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
        cwd: root, encoding: "utf8",
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
    const original = readFileSync(sourcePath);
    if (oraclePath) oracle = readFileSync(oraclePath);
    const mutations = options.mutations ?? [{ patch, diagnostic, label: options.negativeLabel ?? "mutated" }];
    for (const mutation of mutations) {
      step = `check mutation anchors: ${mutation.patch}`;
      const anchor = run("git", ["apply", "--check", mutation.patch]);
      if (anchor.status !== 0) throw new Error(anchor.output);
      try {
        step = `apply production mutation: ${mutation.patch}`;
        const applied = run("git", ["apply", mutation.patch]);
        if (applied.status !== 0) throw new Error(applied.output);
        step = `unchanged oracle rejects production mutation: ${mutation.patch}`;
        const mutated = check(mutation.label);
        require(options.fuzz
          ? mutated.status !== 0 && !mutated.signal && mutated.output.includes(mutation.diagnostic)
            && mutated.output.includes("ERROR: libFuzzer:")
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
    const completed = options.completed ?? (options.fuzz ? ["DONE", `${options.fuzz.runs} runs`, "cov:"] : [options.positive ? `${options.positive.length} tests run: ${options.positive.length} passed`
      : options.nextest ? "1 test run: 1 passed" : "1 passed; 0 failed"]);
    require(restored.status === 0 && !restored.signal && completed.every(marker => restored.output.includes(marker)));
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
