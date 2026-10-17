import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function qualifyMcpLoom(features = ["--features", "hooks,mcp-stdio,mcp-http"], run = spawnSync) {
  return qualifyLoom("acyclic-harness", "mcp-loom", [...features, "--features", "model-checking"], run, {
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_harness_loom --cfg acyclic_loom` }, targetCfg: [],
  });
}

export function qualifyCoordinatorOriginsLoom(features = ["--features", "filesystem-local,native-execution,terminal,mcp-http,grpc,test-support"], run = spawnSync) {
  return qualifyLoom("acyclic-harness", "acyclic_harness", [...features, "--features", "model-checking"], run, {
    kind: "--lib", filter: "distributed::tests::model_checking::origin_publication_fences_competing_writers",
    suites: [
      { filter: "external_executor::task_bindings::model::tests::", exact: false, expected: 3, timeout: 9500 },
      { filter: "external_executor::local_owner::registry::model::tests::", exact: false, expected: 2, timeout: 9500 },
      { filter: "external_executor::local_owner::workers::model::tests::", exact: false, expected: 2, timeout: 9500 },
      { filter: "local::command_views::models::", exact: false, expected: 1, timeout: 9500 },
      { filter: "local::children::preparation::models::", exact: false, expected: 1, timeout: 9500 },
      { filter: "external_executor::local_owner::response::model::tests::", exact: false, expected: 2, timeout: 9500 },
      { filter: "external_executor::local_owner::request_feed::model::tests::", exact: false, expected: 4, timeout: 9500 },
      { filter: "external_executor::native_context::source::models::", exact: false, expected: 1, timeout: 9500 },
      { filter: "task_volume::publication::models::", exact: false, expected: 2, timeout: 9500 },
    ],
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_harness_loom --cfg acyclic_fs_loom --cfg acyclic_loom` }, targetCfg: [],
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

export function qualifyFsBindingLoom(features = [], run = spawnSync) {
  return qualifyLoom("acyclic-fs", "native_binding_model", features, run, {
    targetCfg: ["--", "--cfg", "acyclic_fs_loom"],
  });
}

export function qualifyFsRetryLoom(run = spawnSync) {
  return qualifyLoom("acyclic-fs", "acyclic_fs", [
    "--features", "default,distributed,local,memory,model-checking,native-mount,native-watch",
  ], run, {
    kind: "--lib", filter: "distributed::tests::append_retry::a_lost_cas_does_not_poison_the_rebased_operation_identity",
    timeout: 9500,
    suites: [
      { filter: "git_compat::tests::model::", exact: false, expected: 6, timeout: 9500 },
      { filter: "native_mount::fuse::handles::model::", exact: false, expected: 2, timeout: 9500 },
    ],
    env: { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg acyclic_fs_loom --cfg acyclic_loom` }, targetCfg: [],
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
  const suites = options.filters
    ? options.filters.map(filter => ({ ...options, filter }))
    : [options, ...(options.suites ?? [])];
  for (const suite of suites) {
    const result = run(binaries[0], suite.filter ? [suite.filter, ...(suite.exact === false ? [] : ["--exact"])] : [], { encoding: "utf8", ...(suite.timeout ? { timeout: suite.timeout } : {}) });
    if (result.error) throw result.error;
    process.stdout.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    if (result.status !== 0) throw new Error(`MCP Loom tests failed: ${result.status}`);
    const expected = suite.expected ?? (suite.exact === false ? 4 : 1);
    const complete = suite.filter
      ? new RegExp(`^test result: ok\\. ${expected} passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out;`, "m")
      : /^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;/m;
    if (!complete.test(result.stdout)) {
      throw new Error("MCP Loom qualification requires a nonzero, complete test suite");
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const features = process.argv.slice(2);
  if (features[0] === "--hook-lifecycle") qualifyHookLifecycleLoom(features.length > 1 ? features.slice(1) : undefined);
  else if (features[0] === "--coordinator-origins") qualifyCoordinatorOriginsLoom(features.length > 1 ? features.slice(1) : undefined);
  else if (features[0] === "--summary-checkpoints") qualifySummaryCheckpointsLoom(features.length > 1 ? features.slice(1) : undefined);
  else if (features.length === 1 && features[0] === "--timer") qualifyTimerLoom();
  else if (features.length === 1 && features[0] === "--guest") qualifyGuestLoom();
  else if (features.length === 1 && features[0] === "--memory-accounting") qualifyMemoryAccountingLoom();
  else if (features[0] === "--fs-binding") qualifyFsBindingLoom(features.slice(1));
  else if (features.length === 1 && features[0] === "--fs-retry") qualifyFsRetryLoom();
  else qualifyMcpLoom(features.length ? features : undefined);
}
