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
  { args: ["--native-signal"] }, { args: ["--native-wake"] },
  { args: ["--native-owner"] },
  { args: ["--native-host"] },
  { args: ["--stream-retry"] }, { args: ["--guest"] },
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

export function qualifyProjectionMutationsLoom(run = runCommand) {
  const recovery = "node scripts/qualify-mcp-loom.mjs --projection-mutations";
  return qualifyProjectionSourceMutations([
    ["if slot.is_none() {", "if true {", "projection initialized twice", "first_native_users_share_one_initialization"],
    ["== 0;", ">= 0;", "projection retired under a running command", "the_last_execution_scope_alone_retires_the_projection"],
    ["publish(&mut slot, retire).await", "if retire { publish(&mut slot, retire).await } else { Ok(()) }", "successful command skipped publication", "every_completed_command_publishes_before_its_acknowledgement"],
  ], recovery, run);
}

export function qualifyProjectionCustodyMutationsLoom(run = runCommand) {
  const recovery = "node scripts/qualify-mcp-loom.mjs --projection-custody-mutations";
  return qualifyProjectionSourceMutations([[
    `    let mut slot = lock_owned(StateArc::clone(slot)).await;
    // Keep the completed worker's claim until its publication owns the slot.
    // Another completing command must not retire this projection first.
    drop(entered);`,
    `    drop(entered);
    let mut slot = lock_owned(StateArc::clone(slot)).await;`,
    "completion lost its unpublished projection",
    "every_completing_worker_retains_its_projection_until_publication",
  ], [
    `    let mut slot = lock_owned(StateArc::clone(slot)).await;
    if slot.is_none() {
        *slot = Some(create().await?);
    }
    Ok(slot)`,
    `    let mut slot = std::mem::ManuallyDrop::new(lock_owned(StateArc::clone(slot)).await);
    if slot.is_none() {
        **slot = Some(create().await?);
    }
    Ok(std::mem::ManuallyDrop::into_inner(slot))`,
    "initialization did not wake its registered successor",
    "failed_or_cancelled_initialization_wakes_waiters_without_partial_state",
  ]], recovery, run);
}

export function qualifyNativeScopeMutations(run = runCommand) {
  return qualifyNativeSourceMutation({
    name: "NativeScope", selector: "--native-scope-mutations",
    source: "workspace/volumes", model: "workspace/volumes/publication_tests",
    test: "local::workspace::volumes::publication_tests::failed_scope_entry_retires_the_projections_it_entered",
    mutation: ["let _ = leave_all(std::mem::take(&mut claims.entries)).await;", "claims.entries.clear();",
      "failed later entry stranded the earlier projection"],
  }, run);
}

export function qualifyNativeCancellationMutations(run = runCommand) {
  return qualifyNativeSourceMutation({
    name: "NativeScope", selector: "--native-cancellation-mutations",
    source: "workspace/volumes", model: "workspace/volumes/publication_tests",
    test: "local::workspace::volumes::publication_tests::cancelled_command_retires_before_next_entrant_and_preserves_acknowledged_head",
    mutation: ["claim.source.abandon(claim.entered).await", "{ drop(claim.entered); Ok::<(), Error>(()) }",
      "cancelled command exposed unverified physical bytes"],
  }, run);
}

export function qualifyNativePublicationMutations(run = runCommand) {
  return qualifyNativeSourceMutation({
    name: "NativePublication", selector: "--native-publication-mutations",
    source: "checkout_tools/publisher", model: "checkout_tools/publisher/mount_tests",
    test: "local::checkout_tools::publisher::mount_tests::file_publication_materializes_its_exact_committed_bytes",
    mutation: [
      "if let Some(native) = native {\n                native.advance_to_head().await?;\n            } else if mount.view.source.is_none() {",
      "if let Some(native) = native {\n                let _ = native;\n            } else if mount.view.source.is_none() {",
      "logical replacement did not refresh the active projection",
    ],
  }, run);
}

export function qualifyNativeRetirementMutations(run = runCommand) {
  return qualifyNativeSourceMutation({
    name: "NativeRetirement", selector: "--native-retirement-mutations",
    source: "workspace/volumes", model: "workspace/volumes/publication_tests",
    test: "local::workspace::volumes::publication_tests::retirement_continues_past_a_volume_a_worker_still_owns",
    mutation: [
      "keep_first(&mut retired, Box::pin(source.retire()).await);",
      "Box::pin(source.retire()).await?;",
      "a conflict stranded a later projection",
    ],
  }, run);
}

export function qualifyCaptureCursorMutations(run = runCommand) {
  return qualifyNativeSourceMutation({
    name: "CaptureCursor", selector: "--capture-cursor-mutations",
    source: "checkout_tools/capture", model: "checkout_tools/capture/tests",
    test: "local::checkout_tools::capture::tests::cursor_custody_follows_actual_capture_admission",
    mutations: [[
      "    before_effect().await?;\n    let (physical, view) = selected;",
      "    drop(slot.take());\n    before_effect().await?;\n    let (physical, view) = selected;",
      "capture consumed its cursor before admission",
    ], [
      "let mut cursor = match slot.take() {",
      "let mut cursor = match slot.replace(Cursor::open(physical, policy)?) {",
      "capture retained its cursor after consumed cancellation",
    ]],
  }, run);
}

function qualifyNativeSourceMutation({ name, selector, source, model, test, mutation, mutations = [mutation] }, run) {
  const recovery = `node scripts/qualify-mcp-loom.mjs ${selector}`;
  const qualify = (run, expectedFailure) => qualifyLoom("acyclic-harness", "acyclic_harness",
    ["--features", "filesystem-local,native-execution,test-support"], run, {
      kind: "--lib", modelFeatures: [], targetCfg: [], expectedFailure, recovery, filter: test,
    });
  const root = "../rust/crates/harness/src/local/";
  return qualifySourceMutationsLoom({
    name, recovery, source: `${root}${source}.rs`, model: `${root}${model}.rs`,
    mutations, qualify, restored: run => qualify(run),
  }, run);
}

function qualifyProjectionSourceMutations(mutations, recovery, run) {
  const prefix = "local::workspace::projection::tests::";
  const oracles = Object.fromEntries(mutations.map(([, , message, test]) => [message, `${prefix}${test}`]));
  const qualify = (run, expectedFailure) => qualifyLoom("acyclic-harness", "acyclic_harness",
    ["--all-features"], run, {
      kind: "--lib", env: loomEnvironment(), targetCfg: [], expectedFailure, recovery,
      filters: expectedFailure ? [oracles[expectedFailure]] : Object.values(oracles),
    });
  return qualifySourceMutationsLoom({
    name: "Projection", recovery,
    source: "../rust/crates/harness/src/local/workspace/projection.rs",
    model: "../rust/crates/harness/src/local/workspace/projection/tests.rs",
    mutations,
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

// These models check shared projection ownership; native process cleanup is separate.
export function qualifyHookLifecycleLoom(features = ["--features", "filesystem-local,native-execution,terminal,mcp-http,test-support,grpc"], run = runCommand) {
  return qualifyLoom("acyclic-harness", "acyclic_harness", [...features, "--features", "model-checking"], run, {
    kind: "--lib", filters: [
      "local::workspace::projection::tests::first_native_users_share_one_initialization",
      "local::workspace::projection::tests::failed_or_cancelled_initialization_wakes_waiters_without_partial_state",
      "local::workspace::projection::tests::checkout_then_projection_order_cannot_deadlock",
      "local::workspace::projection::tests::the_last_execution_scope_alone_retires_the_projection",
      "local::workspace::projection::tests::every_completed_command_publishes_before_its_acknowledgement",
      "local::workspace::projection::tests::every_completing_worker_retains_its_projection_until_publication",
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

export function qualifyStreamRetryLoom(run = runCommand) {
  return qualifyLoom("acyclic-stream", "main", [
    "--no-default-features", "--features", "local",
  ], run, {
    env: loomEnvironment(), targetCfg: [], suites: [["retry_recovery_loom::", 2]],
  });
}

export function qualifyGuestLoom(run = runCommand) {
  return qualifyLoom("acyclic-actors", "main", [], run, {
    env: loomEnvironment(), targetCfg: [], suites: [["guest_loom::", 3]],
  });
}

export function qualifyE2eProgressLoom(features = ["--features", "filesystem-local,native-execution,test-support,grpc"], run = runCommand) {
  return qualifyLoom("acyclic-harness", "main", features, run, {
    targetCfg: ["--", "--cfg", "loom"],
    filters: ["e2e_progress_model::registration_reporting_and_cancel_or_panic_cleanup_are_atomic", "e2e_progress_model::forgotten_cleanup_is_detected_on_the_real_registration",
      "e2e_progress_model::polling_wake_and_reporting_never_lose_runnability", "e2e_progress_model::an_unobserved_wake_fails_the_same_runnability_oracle"],
  });
}

const nativeCoreModels = [
  "testing::entropy::models::concurrent_draws_cannot_share_bytes",
  "device_flush::model::every_acknowledgement_follows_a_drain_that_started_after_its_write",
  "device_flush::model::a_failed_drain_acknowledges_nobody_and_strands_nobody",
];

export function qualifyNativeProgressLoom(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [],
    env: loomEnvironment(),
    filters: [
      "testing::model::native_progress_reporting_races_physical_completion_and_unwind",
      "testing::model::accepted_descendants_cannot_escape_cancel_or_lose_the_terminal_wake",
      "testing::queue_model::capacity_release_races_registration_on_the_real_queue",
      "testing::queue_model::physical_completion_races_observer_drop_on_the_real_terminal",
      "testing::queue_model::shared_state_funding_survives_observation_and_worker_release",
      "testing::queue_model::shared_waiter_funding_survives_admission_and_cancellation",
      "testing::queue_model::release_broadcast_reaches_every_waiter_across_concurrent_cancellation",
      "testing::durability::models::concurrent_writers_cannot_clear_the_crash_boundary",
      ...nativeCoreModels,
    ],
  });
}

export function qualifyNativeSignalLoom(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "process_signal_loom", [], run, {
    suites: [["", 2]],
  });
}

export function qualifyNativeDurability(run = runCommand, physical = false) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [],
    ...(physical ? {} : { env: loomEnvironment() }),
    filters: [physical
      ? "testing::durability::tests::every_write_cut_preserves_its_exact_prefix_and_cause"
      : "testing::durability::models::concurrent_writers_cannot_clear_the_crash_boundary"],
  });
}

function qualifyNativeQueueLoom(filters, run) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [], env: loomEnvironment(),
    filters: filters.map(symbol => `testing::queue_model::${symbol}`),
  });
}

function qualifyNativeTrackerLoom(run) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [], env: loomEnvironment(),
    filters: [
      "testing::model::native_progress_reporting_races_physical_completion_and_unwind",
      "testing::model::accepted_descendants_cannot_escape_cancel_or_lose_the_terminal_wake",
    ],
  });
}

export function qualifyNativeWakeLoom(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "wake_loom", [], run, {
    suites: [["", 2]],
  });
}

export function qualifyNativeOwnerLoom(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "owner_count_loom", [], run, {
    suites: [["", 1]],
  });
}

export function qualifyNativeClaimLoom(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", [], run, {
    kind: "--lib", targetCfg: [], env: loomEnvironment(),
    suites: [["claim::model::", 8]],
  });
}

export function qualifyNativeDescriptorLoom(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", [], run, {
    kind: "--lib", targetCfg: [], env: loomEnvironment(),
    suites: [["descriptor::model::", 12]],
  });
}

export function qualifyNativeHostEvents(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [],
    suites: [
      ["testing::host::tests::parking_a_worker_does_not_invent_callback_readiness", 1],
      ["testing::host::tests::unscoped_physical_workers_leave_the_driver_available_to_reply", 1],
    ],
  });
}

export function qualifyNativeOwnerEnvelope(run = runCommand) {
  return qualifyLoom("acyclic-native-runtime", "native_owner_record", [], run, {
    targetCfg: [], filters: ["retained_owner_corpus_satisfies_the_shared_oracle"],
  });
}

export function qualifyNativeHostLoom(run = runCommand, controls = false) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [], env: loomEnvironment(),
    ...(controls ? { filters: [
      "testing::host::models::readiness_racing_worker_parking_cannot_be_lost",
      "testing::host::models::queue_capacity_and_physical_terminal_wakes_are_latched",
      "testing::host::models::physical_occupants_outside_the_schedule_cannot_block_the_driver",
    ] } : { suites: [["testing::host::models::", 4], ["testing::executor::models::", 5]] }),
  });
}

export function qualifyNativeExecutor(run = runCommand, rounds = false) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [],
    ...(rounds ? {} : { env: loomEnvironment() }),
    filters: rounds ? ["testing::executor::tests::every_continuously_ready_task_runs_in_each_round"] : [
      "testing::executor::models::ready_publication_cannot_lose_a_task_or_driver_wake",
      "testing::executor::models::real_completion_wait_latches_early_and_late_wakes",
    ],
  });
}

export function qualifyNativeSources(run = runCommand, core = false) {
  return qualifyLoom("acyclic-native-runtime", "acyclic_native_runtime", ["--features", "test-support"], run, {
    kind: "--lib", targetCfg: [],
    ...(core ? { env: loomEnvironment() } : {}),
    filters: core ? nativeCoreModels : ["testing::clock::tests::automatic_timeout_advancement_cannot_leave_wall_time_behind"],
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
    kind: "--lib", filter: "ownership::model::", exact: false, passed: 6,
    env: loomEnvironment(), targetCfg: [],
  });
}

// Retained workspace suites, including every negative control.
/** @type {Record<string, Array<[string, number]>>} */
export const harnessModelSuites = {
  "acyclic-harness": [
    ["local::workspace::bootstrap::loom_tests::", 1],
    ["local::checkout_tools::tests::", 1],
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
  "--stream-retry": qualifyStreamRetryLoom,
  "--native-signal": qualifyNativeSignalLoom,
  "--native-host": qualifyNativeHostLoom,
  "--native-host-source": run => qualifyNativeHostLoom(run, true),
  "--native-executor-source": qualifyNativeExecutor,
  "--native-executor-rounds": run => qualifyNativeExecutor(run, true),
  "--native-clock": qualifyNativeSources,
  "--native-core-source": run => qualifyNativeSources(run, true),
  "--native-wake": qualifyNativeWakeLoom,
  "--native-owner": qualifyNativeOwnerLoom,
  "--native-claim": qualifyNativeClaimLoom,
  "--native-descriptor": qualifyNativeDescriptorLoom,
  "--native-host-events": qualifyNativeHostEvents,
  "--native-owner-envelope": qualifyNativeOwnerEnvelope,
  "--native-durability": qualifyNativeDurability,
  "--native-durability-io": run => qualifyNativeDurability(run, true),
  "--native-terminal": run => qualifyNativeQueueLoom([
    "physical_completion_races_observer_drop_on_the_real_terminal"], run),
  "--native-capacity": run => qualifyNativeQueueLoom([
    "capacity_release_races_registration_on_the_real_queue",
    "release_broadcast_reaches_every_waiter_across_concurrent_cancellation"], run),
  "--native-funding": run => qualifyNativeQueueLoom([
    "shared_state_funding_survives_observation_and_worker_release",
    "shared_waiter_funding_survives_admission_and_cancellation"], run),
  "--native-tracker": qualifyNativeTrackerLoom,
  "--guest": qualifyGuestLoom,
  "--session-fs": qualifySessionFsLoom, "--ownership": qualifyOwnershipLoom,
  "--native-registry": qualifyNativeRegistryLoom,
  "--skill-materializer": qualifySkillMaterializerLoom,
  "--provider-refresh": qualifyProviderRefreshLoom,
  "--provider-refresh-mutations": qualifyProviderRefreshMutationsLoom,
  "--skill-materializer-mutations": qualifySkillMaterializerMutationsLoom,
  "--projection-mutations": qualifyProjectionMutationsLoom,
  "--projection-custody-mutations": qualifyProjectionCustodyMutationsLoom,
  "--native-scope-mutations": qualifyNativeScopeMutations,
  "--native-cancellation-mutations": qualifyNativeCancellationMutations,
  "--native-publication-mutations": qualifyNativePublicationMutations,
  "--native-retirement-mutations": qualifyNativeRetirementMutations,
  "--capture-cursor-mutations": qualifyCaptureCursorMutations,
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
