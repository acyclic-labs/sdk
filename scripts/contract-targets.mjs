import { spawnSync } from "node:child_process";
import { join } from "node:path";

const identity = target => JSON.stringify(target);

// Cargo remains the authority for source, toolchain, lockfile and feature
// freshness. Never discover or execute a binary by guessing its target path.
/** @param {{ phase?: string, toolchain_ms?: number, rustc_ms?: number, cargo_version?: string, rustc_version?: string, target_directory?: string, metadata_ms?: number, build_ms?: number, artifacts?: { package_id: string, name: string, kinds: string[], features: string[], fresh: boolean, executable: string | null, requested: boolean }[] }} report */
export function buildContractTargets(root, targets, cargo = process.env.ACYCLIC_CARGO_BIN || "cargo", report = {}) {
  if (targets.length === 0) throw new Error("Rust contract target set is empty");
  const capture = (phase, args, executable = cargo) => {
    report.phase = phase;
    const started = performance.now();
    const result = spawnSync(executable, args, {
      cwd: root, encoding: "utf8", maxBuffer: 1 << 28, stdio: ["ignore", "pipe", "inherit"],
    });
    report[`${phase}_ms`] = performance.now() - started;
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`Rust contract command failed with status ${result.status ?? "unknown"}`);
    return result.stdout;
  };
  const manifest = ["--manifest-path", join(root, "Cargo.toml"), "--locked"];
  report.cargo_version = capture("toolchain", ["--version"]).trim();
  // A direct probe is useful context, but Cargo config/wrappers may select a
  // different compiler. Do not present this as the build's compiler identity.
  report.rustc_version = capture("rustc", ["-Vv"], process.env.RUSTC || "rustc").trim();
  const metadata = JSON.parse(capture("metadata", ["metadata", ...manifest, "--no-deps", "--format-version=1"]));
  const packages = new Map(metadata.packages.map(pkg => [pkg.name, pkg]));
  /** @type {Map<string, string | undefined>} */
  const requested = new Map();
  for (const [packageName, kind, name] of targets) {
    const pkg = packages.get(packageName);
    if (!["bin", "example"].includes(kind) || !pkg?.targets.some(target => target.name === name && target.kind.includes(kind))) {
      throw new Error(`unknown Rust contract target: ${identity([packageName, kind, name])}`);
    }
    const key = identity([pkg.id, kind, name]);
    if (requested.has(key)) throw new Error(`duplicate Rust contract target: ${key}`);
    requested.set(key, undefined);
  }
  // Keep the existing shared Cargo cache until configuration-isolation costs
  // have been measured. This path comes from Cargo, including config/env.
  report.target_directory = metadata.target_directory;
  const output = capture("build", [
    "build", ...manifest, "--quiet", "--no-default-features",
    "--target-dir", report.target_directory,
    "--message-format=json-render-diagnostics",
    ...[...new Set(targets.map(([name]) => name))].flatMap(name => ["-p", name]),
    ...targets.flatMap(([, kind, name]) => [`--${kind}`, name]),
  ]);
  report.artifacts = [];
  report.phase = "artifact-discovery";
  for (const line of output.split(/\r?\n/).filter(line => line.startsWith("{"))) {
    const message = JSON.parse(line);
    if (message.reason !== "compiler-artifact") continue;
    const selected = message.target.kind.some(kind => requested.has(identity([message.package_id, kind, message.target.name])));
    report.artifacts.push({ package_id: message.package_id, name: message.target.name, kinds: message.target.kind, features: message.features, fresh: message.fresh, executable: message.executable, requested: selected });
    if (!message.executable) continue;
    for (const kind of message.target.kind) {
      const key = identity([message.package_id, kind, message.target.name]);
      if (!requested.has(key)) continue;
      if (requested.get(key)) throw new Error(`duplicate Rust contract artifact: ${key}`);
      requested.set(key, message.executable);
    }
  }
  return targets.map(([packageName, kind, name]) => {
    const key = identity([packages.get(packageName).id, kind, name]);
    const executable = requested.get(key);
    if (!executable) throw new Error(`Rust contract target was not built: ${key}`);
    return executable;
  });
}

export function runContractTarget(root, executable) {
  const result = spawnSync(executable, { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Rust contract target ${executable} failed with status ${result.status ?? "unknown"}`);
  return result.stdout;
}
