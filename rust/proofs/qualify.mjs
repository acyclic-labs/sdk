#!/usr/bin/env node
// Qualify a production oracle and its retained mutation.
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
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
    ["--features", "filesystem-local,native-execution,test-support,grpc,search-http"],
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
};
const [claim, ...extra] = process.argv.slice(2);
const help = `node rust/proofs/qualify.mjs <${Object.keys(claims).join("|")}>`;
if (claim === "--help" && extra.length === 0) {
  console.log(help);
} else if (!Object.hasOwn(claims, claim) || extra.length !== 0) {
  console.error(`step=select claim; cause=expected exactly one supported claim; recovery: ${help}`);
  process.exitCode = 2;
} else {
  const [source, crate, test, diagnostic, features] = claims[claim];
  const sourcePath = new URL(source, new URL("../../", import.meta.url));
  const patch = claim === "work-mixed"
    ? "rust/proofs/work-arithmetic/mixed-mutation.patch"
    : `rust/proofs/${claim}/mutation.patch`;
  const command = ["test", "--locked", "-p", crate, ...features, "--lib", test, "--", "--exact", "--nocapture"];
  const recovery = `git diff -- ${source}; inspect target/${claim}/*.log; correct source or command, then node rust/proofs/qualify.mjs ${claim}`;
  let step = "read original production source";
  function require(condition) {
    if (!condition) throw new Error("required qualification outcome absent");
  }
  function run(program, args) {
    const result = spawnSync(program, args, {
      cwd: root, encoding: "utf8", maxBuffer: 32 * 1024 * 1024,
      env: { ...process.env, PROPTEST_RNG_SEED: process.env.PROPTEST_RNG_SEED ?? "1697" },
    });
    if (result.error) throw result.error;
    return { ...result, output: `${result.stdout ?? ""}${result.stderr ?? ""}` };
  }
  function check(label) {
    const result = run("cargo", command);
    const directory = new URL(`target/${claim}/`, new URL("../../", import.meta.url));
    mkdirSync(directory, { recursive: true });
    writeFileSync(new URL(`${label}.log`, directory), result.output);
    console.log(`step=${label}; command=${JSON.stringify(["cargo", ...command])}; exit=${result.status}; signal=${result.signal}`);
    console.log(result.output.trimEnd().split(/\r?\n/).slice(-35).join("\n"));
    return result;
  }
  try {
    const original = readFileSync(sourcePath);
    step = "check mutation anchors";
    const anchor = run("git", ["apply", "--check", patch]);
    if (anchor.status !== 0) throw new Error(anchor.output);
    try {
      step = "apply production mutation";
      const applied = run("git", ["apply", patch]);
      if (applied.status !== 0) throw new Error(applied.output);
      step = "unchanged oracle rejects production mutation";
      const mutated = check("mutated");
      require(mutated.status === 101 && !mutated.signal && mutated.output.includes(diagnostic)
        && mutated.output.includes("0 passed; 1 failed"));
    } finally {
      writeFileSync(sourcePath, original);
    }
    step = "restore exact original source";
    require(readFileSync(sourcePath).equals(original));
    step = "restored original oracle completes";
    const restored = check("restored");
    require(restored.status === 0 && !restored.signal && restored.output.includes("1 passed; 0 failed"));
  } catch (error) {
    console.error(`step=${step}; cause=${error.message}; recovery: ${recovery}`);
    process.exitCode = 1;
  }
}
