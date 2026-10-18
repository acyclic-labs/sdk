import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function qualifyMcpLoom(features = ["--features", "hooks,mcp-stdio,mcp-http"], run = spawnSync) {
  return qualifyLoom("acyclic-harness", "mcp-loom", [...features, "--features", "model-checking"], run, {
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_harness_loom --cfg acyclic_loom` }, targetCfg: [],
  });
}

export function qualifyCoordinatorOriginsLoom(features = ["--features", "filesystem-local,native-execution,terminal,mcp-http,test-support"], run = spawnSync) {
  return qualifyLoom("acyclic-harness", "acyclic_harness", [...features, "--features", "model-checking"], run, {
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

export function qualifyHookLifecycleLoom(features = ["--features", "filesystem-local,native-execution,terminal,mcp-http,test-support,grpc"], run = spawnSync) {
  return qualifyLoom("acyclic-harness", "acyclic_harness", [...features, "--features", "model-checking"], run, {
    kind: "--lib", filters: [
      "hooks::effects::models::competing_dispatches_claim_once_with_negative_control",
      "local::checkout_tools::hooks::models::cancellation_keeps_hook_workers_inside_the_selected_workspace",
    ], timeout: 9500,
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_harness_loom --cfg acyclic_loom` }, targetCfg: [],
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
  return qualifyLoom("acyclic-harness", "memory-accounting-model", [
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
      "fresh_deliveries_exhaust_history_replacement"],
  });
}

function qualifyLoom(crate, target, features, run, options = {}) {
  const build = run("cargo", [
    options.kind === "--lib" ? "test" : "rustc", "--locked", "--message-format=json", "-p", crate,
    ...(options.kind === "--lib" ? ["--lib", "--no-run"] : ["--test", target]),
    ...features, ...(options.targetCfg ?? ["--", "--cfg", "loom"]),
  ], { encoding: "utf8", maxBuffer: 4 * 1024 * 1024, ...(options.env ? { env: options.env } : {}) });
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
  for (const filter of options.filters ?? [options.filter]) {
    const result = run(binaries[0], filter ? [filter, ...(options.exact === false ? [] : ["--exact"])] : [], { encoding: "utf8", ...(options.timeout ? { timeout: options.timeout } : {}) });
    process.stdout.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`MCP Loom tests failed: ${result.status}`);
    const complete = options.exact === false
      ? /^test result: ok\. 4 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out;/m
      : filter
      ? /^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out;/m
      : /^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;/m;
    if (!complete.test(result.stdout)) {
      throw new Error("MCP Loom qualification requires a nonzero, complete test suite");
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const features = process.argv.slice(2);
  if (features.length === 1 && features[0] === "--native-progress") qualifyNativeProgressLoom();
  else if (features[0] === "--e2e-progress") qualifyE2eProgressLoom(features.length > 1 ? features.slice(1) : undefined);
  else if (features[0] === "--hook-lifecycle") qualifyHookLifecycleLoom(features.length > 1 ? features.slice(1) : undefined);
  else if (features[0] === "--coordinator-origins") qualifyCoordinatorOriginsLoom(features.length > 1 ? features.slice(1) : undefined);
  else if (features[0] === "--summary-checkpoints") qualifySummaryCheckpointsLoom(features.length > 1 ? features.slice(1) : undefined);
  else if (features.length === 1 && features[0] === "--timer") qualifyTimerLoom();
  else if (features.length === 1 && features[0] === "--guest") qualifyGuestLoom();
  else if (features.length === 1 && features[0] === "--memory-accounting") qualifyMemoryAccountingLoom();
  else if (features.length === 1 && features[0] === "--session-fs") qualifySessionFsLoom();
  else if (features[0] === "--fs-binding") qualifyFsBindingLoom(features.slice(1));
  else qualifyMcpLoom(features.length ? features : undefined);
}
