import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function qualifyMcpLoom(features = ["--features", "hooks,mcp-stdio,mcp-http"], run = spawnSync) {
  return qualifyLoom("acyclic-harness", "mcp-loom", features, run);
}

export function qualifyGuestLoom(run = spawnSync) {
  return qualifyLoom("acyclic-actors", "guest_loom", [], run);
}

export function qualifyFsBindingLoom(features = [], run = spawnSync) {
  return qualifyLoom("acyclic-fs", "native_binding_model", features, run, "acyclic_fs_loom");
}

function qualifyLoom(crate, target, features, run, cfg = "loom") {
  const build = run("cargo", [
    "rustc", "--locked", "--message-format=json", "-p", crate, "--test", target,
    ...features, "--", "--cfg", cfg,
  ], { encoding: "utf8", maxBuffer: 4 * 1024 * 1024 });
  if (build.error) throw build.error;
  if (build.status !== 0) throw new Error(`MCP Loom build failed: ${build.stderr}\n${build.stdout}`);
  const binaries = [];
  for (const line of build.stdout.split(/\r?\n/).filter(Boolean)) {
    const record = JSON.parse(line);
    if (record.reason === "compiler-message") process.stderr.write(record.message.rendered ?? "");
    if (record.reason === "compiler-artifact" && record.target.name === target
      && record.profile.test && record.executable) binaries.push(record.executable);
  }
  if (binaries.length !== 1) throw new Error("MCP Loom build must produce exactly one test binary");
  const result = run(binaries[0], [], { encoding: "utf8" });
  if (result.error) throw result.error;
  process.stdout.write(result.stdout ?? "");
  process.stderr.write(result.stderr ?? "");
  if (result.status !== 0) throw new Error(`MCP Loom tests failed: ${result.status}`);
  if (!/^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;/m.test(result.stdout)) {
    throw new Error("MCP Loom qualification requires a nonzero, complete test suite");
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const features = process.argv.slice(2);
  if (features.length === 1 && features[0] === "--guest") qualifyGuestLoom();
  else if (features[0] === "--fs-binding") qualifyFsBindingLoom(features.slice(1));
  else qualifyMcpLoom(features.length ? features : undefined);
}
