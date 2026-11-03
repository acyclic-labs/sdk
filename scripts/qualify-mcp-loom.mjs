import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

/** @typedef {(command: string, args: string[], options: {
 * encoding: 'utf8', maxBuffer?: number, env?: NodeJS.ProcessEnv
 * }) => {status: number | null, stdout: string, stderr?: string, error?: Error}} RunCommand */
/** @type {RunCommand} */
const runCommand = spawnSync;

// The gate and its offline preflight consume this same ordered step list.
export const gateQualifications = [
  { args: ["--all-features"] },
  { args: ["--hook-lifecycle", "--all-features"] },
  { args: ["--native-registry"] },
  { args: ["--e2e-progress"] }, { args: ["--native-progress"] },
  { args: ["--guest"] },
  { args: ["--fs-binding", "--all-features"] },
  { args: ["--fs-policy"] },
  { args: ["--session-fs"] }, { args: ["--ownership"] },
  { args: ["--skill-materializer"] },
  { args: ["--provider-refresh"] },
  { args: ["--harness-models", "--all-features"] },
];
// Cargo's workspace wrapper leaves registry dependencies (including Tokio's own
// independent cfg(loom) mode) untouched. One opt-in compiles each SDK model crate.
export function loomEnvironment(env = process.env) {
  return { ...env, RUSTC_WORKSPACE_WRAPPER: fileURLToPath(new URL(
    process.platform === "win32" ? "./loom-rustc.cmd" : "./loom-rustc.sh", import.meta.url)) };
}
const planOnly = Symbol("qualification plan");

export function qualificationPlan(args) {
  return dispatch(args, planOnly);
}

export function qualificationGatePlans() {
  return qualifyGate(planOnly);
}

function qualifyGate(run) {
  return gateQualifications.map(step => dispatch(step.args, run));
}

export function qualifyMcpLoom(features = ["--features", "hooks,mcp-stdio,mcp-http"], run = runCommand) {
  return qualifyLoom("acyclic-harness-core", "main", [...features, "--features", "model-checking"], run, {
    env: loomEnvironment(), targetCfg: [], suites: [["mcp_loom::", 6]],
  });
}

export function qualifySkillMaterializerLoom(run = runCommand) {
  return qualifyLoom("acyclic-harness-core", "main",
    ["--features", "filesystem-local,native-execution,test-support"], run, {
      targetCfg: ["--", "--cfg", "loom"],
      filters: ["skill_materializer_loom::journal_owner_serializes_first_publications", "skill_materializer_loom::cancelled_waiter_does_not_take_the_next_owners_wake"],
    });
}

export function qualifyProviderRefreshLoom(run = runCommand) {
  return qualifyLoom("acyclic-harness-providers", "main", [], run, {
    modelFeatures: [],
    suites: [["verification_models::refresh::", 3]],
  });
}

export function qualifyProviderRefreshMutationsLoom(run = runCommand) {
  const recovery = "node scripts/qualify-mcp-loom.mjs --provider-refresh-mutations";
  return qualifySourceMutationsLoom({
    name: "Refresh", recovery,
    source: "../rust/crates/harness-providers/src/refresh.rs",
    model: "../rust/crates/harness-providers/tests/models/refresh.rs",
    mutations: [
      ["return Err(failure);", "let _ = failure;", "failed refresh was repeated by a waiter"],
      ["lock_owned(self.result.clone()).await", "lock_owned(Arc::new(Mutex::new(None))).await", "refresh ownership overlapped"],
    ],
    qualify: (run, expectedFailure) => qualifyLoom("acyclic-harness-providers", "main", [], run, {
      modelFeatures: [], expectedFailure, recovery,
      filter: "verification_models::refresh::refresh_attempt_failure_retains_exclusive_ownership",
    }),
    restored: qualifyProviderRefreshLoom,
  }, run);
}

export function qualifySkillMaterializerMutationsLoom(run = runCommand) {
  const recovery = "node scripts/qualify-mcp-loom.mjs --skill-materializer-mutations";
  return qualifySourceMutationsLoom({
    name: "SkillMaterializer", recovery,
    source: "../rust/crates/harness-core/src/context/skill_materializer.rs",
    model: "../rust/crates/harness-core/tests/skill_materializer_loom.rs",
    mutations: [["lock_owned(self.0.clone()).await", "lock_owned(Arc::new(Mutex::new(()))).await", "materialization conflict"]],
    qualify: (run, expectedFailure) => qualifyLoom("acyclic-harness-core", "main",
      ["--features", "filesystem-local,native-execution,test-support"], run, {
        targetCfg: ["--", "--cfg", "loom"], expectedFailure, recovery,
        filter: "skill_materializer_loom::journal_owner_serializes_first_publications",
      }),
    restored: qualifySkillMaterializerLoom,
  }, run);
}

export function qualifyProjectionInitializationMutationsLoom(run = runCommand) {
  const recovery = "node scripts/qualify-mcp-loom.mjs --projection-initialization-mutations";
  const qualify = (run, expectedFailure) => qualifyLoom("acyclic-harness", "acyclic_harness",
    ["--all-features"], run, {
      kind: "--lib", env: loomEnvironment(), targetCfg: [], expectedFailure, recovery,
      filter: "local::workspace::projection::tests::first_native_users_share_one_initialization",
    });
  return qualifySourceMutationsLoom({
    name: "Projection initialization", recovery,
    source: "../rust/crates/harness/src/local/workspace/projection.rs",
    model: "../rust/crates/harness/src/local/workspace/projection/tests.rs",
    mutations: [["if slot.is_none() {", "if true {", "projection initialized twice"]],
    qualify, restored: run => qualify(run),
  }, run);
}

export function qualifyProjectionLifetimeMutationsLoom(run = runCommand) {
  const recovery = "node scripts/qualify-mcp-loom.mjs --projection-lifetime-mutations";
  const prefix = "local::workspace::projection::tests::";
  const oracles = {
    "projection retired under a running command": `${prefix}the_last_execution_scope_alone_retires_the_projection`,
    "successful command skipped publication": `${prefix}every_completed_command_publishes_before_its_acknowledgement`,
  };
  const qualify = (run, expectedFailure) => qualifyLoom("acyclic-harness", "acyclic_harness",
    ["--all-features"], run, {
      kind: "--lib", env: loomEnvironment(), targetCfg: [], expectedFailure, recovery,
      filters: expectedFailure ? [oracles[expectedFailure]] : Object.values(oracles),
    });
  return qualifySourceMutationsLoom({
    name: "Projection lifetime", recovery,
    source: "../rust/crates/harness/src/local/workspace/projection.rs",
    model: "../rust/crates/harness/src/local/workspace/projection/tests.rs",
    mutations: [
      ["== 0;", ">= 0;", "projection retired under a running command"],
      ["publish(&mut slot, retire).await", "if retire { publish(&mut slot, retire).await } else { Ok(()) }", "successful command skipped publication"],
    ],
    qualify, restored: run => qualify(run),
  }, run);
}

// Each control changes an exact entrance in the included production source;
// the model and its semantic assertion stay byte-identical across the build.
function qualifySourceMutationsLoom({ name, source, model, mutations, qualify, restored, recovery }, run) {
  if (run === planOnly) return mutations.map(([, , message]) => qualify(run, message));
  const path = new URL(source, import.meta.url);
  const fail = (step, detail) => {
    throw new Error(`${name} ${step}: ${detail}\nRecovery after correcting the cause: ${recovery}`);
  };
  let original;
  try { original = readFileSync(path, "utf8"); }
  catch (error) { fail("read production source", String(error)); }
  const hash = source => createHash("sha256").update(source).digest("hex");
  try {
    console.log(`${name} unchanged model=${hash(readFileSync(new URL(model, import.meta.url)))}`);
    for (const [before, after, expectedFailure] of mutations) {
      if (original.split(before).length !== 2) fail("source mutation", "expected exactly one original source entrance");
      const planted = original.replace(before, after);
      writeFileSync(path, planted);
      console.log(`${name}: original=${hash(original)} planted=${hash(planted)}`);
      qualify(run, expectedFailure);
    }
  } catch (error) {
    fail("source mutation qualification", String(error));
  } finally {
    try {
      writeFileSync(path, original);
      if (readFileSync(path, "utf8") !== original) fail("restore production source", "original bytes differ");
      console.log(`PASS: original ${name} source restored ${hash(original)}`);
    } catch (error) { fail("restore production source", String(error)); }
  }
  try { restored(run); }
  catch (error) { fail("restored production qualification", String(error)); }
}

// Checkout and projection models retain physical cleanup custody.
export function qualifyHookLifecycleLoom(features = ["--features", "filesystem-local,native-execution,terminal,mcp-http,test-support,grpc"], run = runCommand) {
  return qualifyLoom("acyclic-harness", "acyclic_harness", [...features, "--features", "model-checking"], run, {
    kind: "--lib", filters: [
      "local::checkout_tools::tests::cancelled_bash_retains_projection_until_physical_cleanup",
      "local::workspace::projection::tests::first_native_users_share_one_initialization",
      "local::workspace::projection::tests::first_projection_and_logical_publication_share_custody",
      "local::workspace::projection::tests::failed_initialization_leaves_no_published_projection",
      "local::workspace::projection::tests::cancelling_initialization_releases_waiters_without_partial_state",
      "local::workspace::projection::tests::checkout_then_projection_order_cannot_deadlock",
      "local::workspace::projection::tests::the_last_execution_scope_alone_retires_the_projection",
      "local::workspace::projection::tests::every_completed_command_publishes_before_its_acknowledgement",
      "local::workspace::projection::tests::every_completing_worker_retains_its_projection_until_publication",
      "local::workspace::projection::tests::execution_guard_covers_physical_cleanup_and_unpublished_retirement",
    ],
    env: loomEnvironment(), targetCfg: [],
  });
}

export function qualifyNativeRegistryLoom(run = runCommand) {
  return qualifyLoom("acyclic-fs", "acyclic_fs", ["--features", "model-checking"], run, {
    kind: "--lib", filters: [
      "native_mount::fence::models::registry_contention_retires_with_original_credit_and_missing_wait_control",
      "native_mount::fence::models::retirement_preserves_registry_identity_for_pending_and_new_starters",
    ],
    env: loomEnvironment(), targetCfg: [],
  });
}

export function qualifyGuestLoom(run = runCommand) {
  return qualifyLoom("acyclic-actors", "main", [], run, {
    env: loomEnvironment(), targetCfg: [], suites: [["guest_loom::", 4]],
  });
}

export function qualifyE2eProgressLoom(features = ["--features", "filesystem-local,native-execution,test-support,grpc"], run = runCommand) {
  return qualifyLoom("acyclic-harness", "main", features, run, {
    targetCfg: ["--", "--cfg", "loom"],
    filters: ["e2e_progress_model::registration_reporting_and_cancel_or_panic_cleanup_are_atomic", "e2e_progress_model::forgotten_cleanup_is_detected_on_the_real_registration",
      "e2e_progress_model::polling_wake_and_reporting_never_lose_runnability", "e2e_progress_model::an_unobserved_wake_fails_the_same_runnability_oracle"],
  });
}

export function qualifyNativeProgressLoom(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [],
    env: loomEnvironment(),
    filters: [
      "testing::model::native_progress_reporting_races_physical_completion_and_unwind",
      "testing::model::missing_native_cleanup_is_detected_on_the_actual_tracker",
      "testing::model::accepted_descendants_cannot_escape_cancel_or_lose_the_terminal_wake",
      "testing::model::missing_registration_is_detected_on_the_real_tracker",
      "testing::model::missing_terminal_wake_is_detected_on_the_real_tracker",
      "testing::queue_model::capacity_release_races_registration_on_the_real_queue",
      "testing::queue_model::physical_completion_races_observer_drop_on_the_real_terminal",
      "testing::queue_model::broken_capacity_wake_is_detected",
      "testing::queue_model::broken_capacity_accounting_is_detected",
      "testing::queue_model::broken_terminal_wake_is_detected",
      "testing::queue_model::broken_panicked_terminal_wake_is_detected",
      "testing::queue_model::broken_failed_terminal_wake_is_detected",
      "testing::queue_model::broken_terminal_accounting_is_detected",
      "testing::queue_model::broken_panicked_terminal_accounting_is_detected",
      "testing::queue_model::broken_failed_terminal_accounting_is_detected",
      "testing::queue_model::replaced_worker_failure_cause_is_detected",
      "testing::queue_model::shared_state_funding_survives_observation_and_worker_release",
      "testing::queue_model::premature_shared_state_refund_is_detected",
      "testing::queue_model::premature_panicked_shared_state_refund_is_detected",
      "testing::queue_model::premature_abandoned_panicked_state_refund_is_detected",
      "testing::queue_model::shared_waiter_funding_survives_admission_and_cancellation",
      "testing::queue_model::premature_admitted_waiter_refund_is_detected",
      "testing::queue_model::premature_cancelled_waiter_refund_is_detected",
      "testing::queue_model::release_broadcast_reaches_every_waiter_across_concurrent_cancellation",
      "testing::queue_model::head_only_capacity_wake_is_detected",
    ],
  });
}

export function qualifyFsBindingLoom(features = [], run = runCommand) {
  return qualifyLoom("acyclic-fs", "main", features, run, {
    targetCfg: ["--", "--cfg", "loom"], suites: [["native_binding_model::", 5]],
  });
}

export function qualifyFsPolicyLoom(run = runCommand) {
  return qualifyLoom("acyclic-fs", "acyclic_fs", [
    "--no-default-features", "--features", "memory,model-checking",
  ], run, {
    kind: "--lib", targetCfg: [],
    env: loomEnvironment(),
    suites: [["facade::policy_cache::loom_tests::", 2], ["distributed::head_payload_loom_tests::", 8]],
  });
}

export function qualifySessionFsLoom(run = runCommand) {
  return qualifyLoom("acyclic-fs", "main", [
    "--no-default-features", "--features", "memory,session,acyclic-stream/local",
  ], run, {
    env: loomEnvironment(),
    targetCfg: [],
    filters: ["session_fs_model::change_resume_exhausts_append_registration_and_wakeup",
      "session_fs_model::change_resume_exhausts_independent_history_replacement"],
  });
}

export function qualifyOwnershipLoom(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", [], run, {
    kind: "--lib", filter: "ownership::model::", exact: false, passed: 8,
    env: loomEnvironment(), targetCfg: [],
  });
}

// Retained workspace suites, including every negative control.
/** @type {Record<string, Array<[string, number]>>} */
export const harnessModelSuites = {
  "acyclic-harness": [
    ["local::workspace::bootstrap::loom_tests::", 1],
    ["local::checkout_tools::tests::", 5],
  ],
};

export function qualifyHarnessModelsLoom(features = ["--all-features"], run = runCommand) {
  return qualifyHarnessSuite("acyclic-harness", "acyclic_harness", features, run, {
    kind: "--lib", suites: harnessModelSuites["acyclic-harness"],
  });
}

function qualifyHarnessSuite(crate, target, features, run, options = {}) {
  return qualifyLoom(crate, target, [...features, "--features", "model-checking"], run, {
    ...options,
    env: loomEnvironment(),
    targetCfg: [],
  });
}

function qualifyLoom(crate, target, features, run, options = {}) {
  const args = [
    options.kind === "--lib" ? "test" : "rustc", "--locked", "--message-format=json", "-p", crate,
    ...(options.kind === "--lib" ? ["--lib", "--no-run"] : ["--test", target]),
    ...features, ...(options.modelFeatures ?? ["--features", "model-checking"]),
    ...(options.targetCfg ?? ["--", "--cfg", "loom"]),
  ];
  const tests = options.suites?.map(([filter, passed]) => ({ args: [filter], passed })) ?? (options.filters ?? [options.filter]).map(filter => ({
    args: filter ? [filter, ...(options.exact === false ? [] : ["--exact"])] : [],
    passed: options.exact === false ? (options.passed ?? 4) : filter ? 1 : null,
  }));
  const env = options.env ?? process.env;
  const plan = { args, target, tests, rustflags: env.RUSTFLAGS ?? "", wrapper: env.RUSTC_WORKSPACE_WRAPPER ?? null };
  if (options.expectedFailure) plan.expectedFailure = options.expectedFailure;
  if (run === planOnly) return plan;
  const build = run("cargo", args, { encoding: "utf8", maxBuffer: 4 * 1024 * 1024, ...(options.env ? { env: options.env } : {}) });
  if (build.error) throw build.error;
  if (build.status !== 0) {
    const diagnostics = build.stdout.split(/\r?\n/).filter(Boolean).flatMap(line => {
      try { const record = JSON.parse(line); return record.reason === "compiler-message" ? [record.message.rendered ?? ""] : []; }
      catch { return [line]; }
    }).join("\n");
    throw new Error(`MCP Loom build failed: ${build.stderr}\n${diagnostics}`);
  }
  const binaries = [];
  for (const line of build.stdout.split(/\r?\n/).filter(Boolean)) {
    const record = JSON.parse(line);
    if (record.reason === "compiler-message") process.stderr.write(record.message.rendered ?? "");
    if (record.reason === "compiler-artifact" && record.target.name === target
      && record.profile.test && record.executable) binaries.push(record.executable);
  }
  if (binaries.length !== 1) throw new Error("MCP Loom build must produce exactly one test binary");
  for (const test of tests) {
    const result = run(binaries[0], test.args, { encoding: "utf8" });
    if (result.error) throw result.error;
    process.stdout.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    if (result.error) throw result.error;
    if (options.expectedFailure) {
      const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
      if (result.status !== 101 || !output.includes(options.expectedFailure)
        || !/^test result: FAILED\. 0 passed; 1 failed; 0 ignored; 0 measured; [0-9]+ filtered out;/m.test(output)) {
        throw new Error(`Production source mutation: unchanged semantic oracle did not fail as required\nRecovery after correcting the cause: ${options.recovery}`);
      }
      console.log(`PASS: production mutation reached unchanged oracle: ${options.expectedFailure}`);
      continue;
    }
    if (result.status !== 0) throw new Error(`MCP Loom tests failed: ${result.status}`);
    const complete = test.passed === null
      ? /^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;/m
      : new RegExp(`^test result: ok\\. ${test.passed} passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out;`, "m");
    if (!complete.test(result.stdout)) {
      throw new Error("MCP Loom qualification requires a nonzero, complete test suite");
    }
  }
}

// Every model flag is named here; anything else is a cargo feature selection or an error.
const fixedModels = {
  "--native-progress": qualifyNativeProgressLoom,
  "--guest": qualifyGuestLoom,
  "--session-fs": qualifySessionFsLoom, "--ownership": qualifyOwnershipLoom,
  "--native-registry": qualifyNativeRegistryLoom,
  "--skill-materializer": qualifySkillMaterializerLoom,
  "--provider-refresh": qualifyProviderRefreshLoom,
  "--provider-refresh-mutations": qualifyProviderRefreshMutationsLoom,
  "--skill-materializer-mutations": qualifySkillMaterializerMutationsLoom,
  "--projection-initialization-mutations": qualifyProjectionInitializationMutationsLoom,
  "--projection-lifetime-mutations": qualifyProjectionLifetimeMutationsLoom,
  "--fs-policy": qualifyFsPolicyLoom,
};
const selectableModels = {
  "--e2e-progress": qualifyE2eProgressLoom, "--hook-lifecycle": qualifyHookLifecycleLoom,
  "--fs-binding": qualifyFsBindingLoom,
  "--harness-models": qualifyHarnessModelsLoom,
};

function featureSelection(args) {
  for (let index = 0; index < args.length; index++) {
    if (args[index] === "--all-features" || args[index] === "--no-default-features") continue;
    if (args[index] === "--features" && /^[^-]/.test(args[index + 1] ?? "")) { index++; continue; }
    throw new Error(`unknown qualification argument: ${args[index]}`);
  }
  return args.length ? args : undefined;
}

/** @param {string[]} args */
function dispatch([name, ...rest], run) {
  if (Object.hasOwn(fixedModels, name) && rest.length === 0) return fixedModels[name](run);
  if (Object.hasOwn(selectableModels, name)) return selectableModels[name](featureSelection(rest), run);
  return qualifyMcpLoom(featureSelection(name === undefined ? [] : [name, ...rest]), run);
}

export function qualificationMain(args, run = runCommand) {
  if (args[0] === "--gate") {
    if (args.length !== 1) throw new Error(`unknown qualification argument: ${args[1]}`);
    qualifyGate(run);
  } else dispatch(args, run);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  qualificationMain(process.argv.slice(2));
}
