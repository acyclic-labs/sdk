import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

// The gate and its offline preflight consume this same ordered step list.
export const gateQualifications = [
  { args: ["--all-features"] },
  { args: ["--coordinator-origins", "--all-features"] },
  { args: ["--summary-checkpoints", "--all-features"] },
  { args: ["--hook-effects", "--all-features"] },
  { args: ["--hook-lifecycle", "--all-features"] },
  { args: ["--native-registry"] },
  { args: ["--e2e-progress"] }, { args: ["--native-progress"] },
  { args: ["--timer"] }, { args: ["--guest"] },
  { args: ["--fs-binding", "--all-features"] },
  { args: ["--memory-accounting"], rustflags: "-Dwarnings" },
  { args: ["--session-fs"] }, { args: ["--ownership"] },
  { args: ["--skill-materializer"] },
];
const planOnly = Symbol("qualification plan");

export function qualificationPlan(args) {
  return dispatch(args, planOnly);
}

export function qualificationGatePlans() {
  return qualifyGate(planOnly);
}

function qualifyGate(run) {
  return gateQualifications.map(step => {
    const previous = process.env.RUSTFLAGS;
    try {
      if (step.rustflags) process.env.RUSTFLAGS = `${previous ?? ""} ${step.rustflags}`;
      return dispatch(step.args, run);
    } finally {
      if (previous === undefined) delete process.env.RUSTFLAGS;
      else process.env.RUSTFLAGS = previous;
    }
  });
}

export function qualifyMcpLoom(features = ["--features", "hooks,mcp-stdio,mcp-http"], run = spawnSync) {
  return qualifyLoom("acyclic-harness-core", "mcp-loom", [...features, "--features", "model-checking"], run, {
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_harness_loom --cfg acyclic_loom` }, targetCfg: [],
  });
}

export function qualifySkillMaterializerLoom(run = spawnSync) {
  return qualifyLoom("acyclic-harness-core", "skill-materializer-loom",
    ["--features", "filesystem-local,native-execution,test-support"], run, {
      targetCfg: ["--", "--cfg", "acyclic_harness_loom"], timeout: 9500,
      filters: ["journal_owner_serializes_first_publications", "former_independent_publishers_can_conflict"],
    });
}

export function qualifyCoordinatorOriginsLoom(features = ["--features", "filesystem-local,native-execution,terminal,mcp-http,test-support"], run = spawnSync) {
  return qualifyLoom("acyclic-harness-core", "acyclic_harness_core", [...features, "--features", "model-checking"], run, {
    kind: "--lib", filter: "distributed::tests::model_checking::origin_publication_fences_competing_writers",
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_harness_loom --cfg acyclic_loom` }, targetCfg: [],
  });
}

export function qualifySummaryCheckpointsLoom(features = ["--features", "filesystem-local,native-execution,terminal,mcp-http,test-support"], run = spawnSync) {
  return qualifyLoom("acyclic-harness", "acyclic_harness", [...features, "--features", "model-checking"], run, {
    kind: "--lib", filter: "local::compact::checkpoints::model::",
    exact: false, timeout: 9500,
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_harness_loom --cfg acyclic_loom` }, targetCfg: [],
  });
}

// Hook dispatch claims live in acyclic-harness-core; the local checkout and
// projection hook models live in the acyclic-harness facade.
export function qualifyHookEffectsLoom(features = ["--features", "filesystem-local,native-execution,terminal,mcp-http,test-support,grpc"], run = spawnSync) {
  return qualifyLoom("acyclic-harness-core", "acyclic_harness_core", [...features, "--features", "model-checking"], run, {
    kind: "--lib", filters: [
      "hooks::effects::models::competing_dispatches_claim_once_with_negative_control",
    ], timeout: 9500,
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_harness_loom --cfg acyclic_loom` }, targetCfg: [],
  });
}

export function qualifyHookLifecycleLoom(features = ["--features", "filesystem-local,native-execution,terminal,mcp-http,test-support,grpc"], run = spawnSync) {
  return qualifyLoom("acyclic-harness", "acyclic_harness", [...features, "--features", "model-checking"], run, {
    kind: "--lib", filters: [
      "local::checkout_tools::hooks::models::cancellation_keeps_hook_workers_inside_the_selected_workspace",
      "local::checkout_tools::hooks::models::cancelled_hook_retains_projection_claim_until_physical_cleanup",
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
    ], timeout: 9500,
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_harness_loom --cfg acyclic_loom` }, targetCfg: [],
  });
}

export function qualifyNativeRegistryLoom(run = spawnSync) {
  return qualifyLoom("acyclic-fs", "acyclic_fs", ["--features", "model-checking"], run, {
    kind: "--lib", filters: [
      "native_mount::fence::models::registry_contention_retires_with_original_credit_and_missing_wait_control",
      "native_mount::fence::models::retirement_preserves_registry_identity_for_pending_and_new_starters",
    ], timeout: 9500,
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_fs_native_loom` }, targetCfg: [],
  });
}

export function qualifyTimerLoom(run = spawnSync) {
  return qualifyLoom("acyclic-actors", "timer_loom", [
    "--features", "acyclic-stream/local", "--target-dir", resolve(process.env.CARGO_TARGET_DIR || "target", "timer-loom"),
  ], run, {
    env: { ...process.env, RUSTFLAGS: "--cfg acyclic_loom" }, targetCfg: [],
  });
}

export function qualifyGuestLoom(run = spawnSync) {
  return qualifyLoom("acyclic-actors", "guest_loom", [], run);
}

export function qualifyE2eProgressLoom(features = ["--features", "filesystem-local,native-execution,test-support,grpc"], run = spawnSync) {
  return qualifyLoom("acyclic-harness", "e2e-progress-model", features, run, {
    targetCfg: ["--", "--cfg", "acyclic_e2e_progress_loom"],
    filters: ["registration_reporting_and_cancel_or_panic_cleanup_are_atomic", "forgotten_cleanup_is_detected_on_the_real_registration",
      "polling_wake_and_reporting_never_lose_runnability", "an_unobserved_wake_fails_the_same_runnability_oracle"],
    timeout: 9500,
  });
}

export function qualifyNativeProgressLoom(run = spawnSync) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [],
    env: {...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_native_runtime_loom`},
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
      "testing::queue_model::broken_terminal_accounting_is_detected",
      "testing::queue_model::observer_retires_the_state_charge_before_the_worker_lets_go",
      "testing::queue_model::a_charge_left_on_the_shared_state_is_detected",
    ], timeout: 9500,
  });
}

export function qualifyFsBindingLoom(features = [], run = spawnSync) {
  return qualifyLoom("acyclic-fs", "native_binding_model", features, run, {
    targetCfg: ["--", "--cfg", "acyclic_fs_loom"],
  });
}

export function qualifyMemoryAccountingLoom(run = spawnSync) {
  return qualifyLoom("acyclic-harness-core", "memory-accounting-model", [
    "--features", "filesystem,acyclic-fs/model-checking",
    "--target-dir", resolve(process.env.CARGO_TARGET_DIR || "target", "memory-accounting-loom"),
  ], run, {
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_fs_loom` },
    targetCfg: ["--", "--cfg", "acyclic_harness_loom"],
  });
}

export function qualifySessionFsLoom(run = spawnSync) {
  return qualifyLoom("acyclic-fs", "session_fs_model", [
    "--no-default-features", "--features", "memory,session,acyclic-stream/local",
  ], run, {
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_loom --cfg acyclic_stream_loom` },
    targetCfg: [], timeout: 9500,
    filters: ["checked_resume_exhausts_append_registration_and_wakeup",
      "checked_resume_exhausts_independent_history_replacement", "resumed_buffered_delivery_exhausts_revocation",
      "fresh_open_exhausts_deletion_and_history_replacement",
      "fresh_deliveries_exhaust_history_replacement",
      "terminal_opening_exhausts_deletion_registration",
      "terminal_watch_exhausts_zero_credit_deletion"],
  });
}

export function qualifyOwnershipLoom(run = spawnSync) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", [], run, {
    kind: "--lib", filter: "ownership::model::", exact: false, passed: 8, timeout: 9500,
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_native_runtime_loom` }, targetCfg: [],
  });
}

function qualifyLoom(crate, target, features, run, options = {}) {
  const args = [
    options.kind === "--lib" ? "test" : "rustc", "--locked", "--message-format=json", "-p", crate,
    ...(options.kind === "--lib" ? ["--lib", "--no-run"] : ["--test", target]),
    ...features, ...(options.targetCfg ?? ["--", "--cfg", "loom"]),
  ];
  const tests = (options.filters ?? [options.filter]).map(filter => ({
    args: filter ? [filter, ...(options.exact === false ? [] : ["--exact"])] : [],
    passed: options.exact === false ? (options.passed ?? 4) : filter ? 1 : null,
  }));
  const env = options.env ?? process.env;
  const plan = { args, target, tests, rustflags: env.RUSTFLAGS ?? "" };
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
    const result = run(binaries[0], test.args, { encoding: "utf8", ...(options.timeout ? { timeout: options.timeout } : {}) });
    if (result.error) throw result.error;
    process.stdout.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    if (result.error) throw result.error;
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
  "--native-progress": qualifyNativeProgressLoom, "--timer": qualifyTimerLoom,
  "--guest": qualifyGuestLoom, "--memory-accounting": qualifyMemoryAccountingLoom,
  "--session-fs": qualifySessionFsLoom, "--ownership": qualifyOwnershipLoom,
  "--native-registry": qualifyNativeRegistryLoom,
  "--skill-materializer": qualifySkillMaterializerLoom,
};
const selectableModels = {
  "--e2e-progress": qualifyE2eProgressLoom, "--hook-effects": qualifyHookEffectsLoom, "--hook-lifecycle": qualifyHookLifecycleLoom,
  "--coordinator-origins": qualifyCoordinatorOriginsLoom, "--summary-checkpoints": qualifySummaryCheckpointsLoom,
  "--fs-binding": qualifyFsBindingLoom,
};

function featureSelection(args) {
  for (let index = 0; index < args.length; index++) {
    if (args[index] === "--all-features" || args[index] === "--no-default-features") continue;
    if (args[index] === "--features" && /^[^-]/.test(args[index + 1] ?? "")) { index++; continue; }
    throw new Error(`unknown qualification argument: ${args[index]}`);
  }
  return args.length ? args : undefined;
}

function dispatch([name, ...rest], run) {
  if (Object.hasOwn(fixedModels, name) && rest.length === 0) return fixedModels[name](run);
  if (Object.hasOwn(selectableModels, name)) return selectableModels[name](featureSelection(rest), run);
  return qualifyMcpLoom(featureSelection(name === undefined ? [] : [name, ...rest]), run);
}

export function qualificationMain(args, run = spawnSync) {
  if (args[0] === "--gate") {
    if (args.length !== 1) throw new Error(`unknown qualification argument: ${args[1]}`);
    qualifyGate(run);
  } else dispatch(args, run);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  qualificationMain(process.argv.slice(2));
}
