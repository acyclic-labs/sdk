import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFile, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function command(executable, args = []) {
  try {
    return execFileSync(executable, args, { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  } catch {
    return undefined;
  }
}

function probe(executable, args = []) {
  try {
    return execFileSync(executable, args, { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  } catch (error) {
    const output = `${error.stdout ?? ""}${error.stderr ?? ""}`.trim();
    return output === "" ? undefined : output;
  }
}

function linkerCandidates() {
  const executable = process.platform === "win32" ? "where.exe" : "which";
  const candidates = command(executable, ["link.exe"])?.split(/\r?\n/).map(value => value.trim()).filter(Boolean) ?? [];
  if (process.platform === "win32") {
    const vswhere = process.env.PROGRAMFILES_X86 === undefined
      ? "C:/Program Files (x86)/Microsoft Visual Studio/Installer/vswhere.exe"
      : `${process.env.PROGRAMFILES_X86}/Microsoft Visual Studio/Installer/vswhere.exe`;
    const visualStudio = command(vswhere, ["-latest", "-products", "*", "-requires", "Microsoft.VisualStudio.Component.VC.Tools.x86.x64", "-find", "**/Hostx64/x64/link.exe"]);
    if (visualStudio !== undefined) candidates.unshift(...visualStudio.split(/\r?\n/).map(value => value.trim()).filter(Boolean));
  }
  return [...new Set(candidates)];
}

async function digest(path) {
  const bytes = await readFile(resolve(root, path));
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

async function exists(path) {
  try { await stat(path); return true; } catch { return false; }
}

function packageVersion(packageJson, packageName) {
  const version = packageJson.devDependencies?.[packageName] ?? packageJson.dependencies?.[packageName];
  return typeof version === "string" ? version : undefined;
}

function profileIncremental(cargoToml) {
  const match = cargoToml.match(/\[profile\.release\][\s\S]*?\nincremental\s*=\s*(true|false)/);
  return match?.[1] === "true" ? true : match?.[1] === "false" ? false : undefined;
}

function configuredRustflags(cargoConfig) {
  const match = cargoConfig.match(/rustflags\s*=\s*\[([^\]]*)\]/s);
  return match?.[1]?.match(/"([^"]+)"/g)?.map(value => value.slice(1, -1)) ?? [];
}

export async function build_inputs({ target = process.env.STREAM_NATIVE_TARGET ?? "x86_64-pc-windows-msvc", targetDir = process.env.CARGO_TARGET_DIR ?? resolve(root, "target") } = {}) {
  const packageJson = JSON.parse(await readFile(resolve(root, "package.json"), "utf8"));
  const cargoToml = await readFile(resolve(root, "Cargo.toml"), "utf8");
  const cargoConfig = await readFile(resolve(root, ".cargo/config.toml"), "utf8");
  const rustToolchain = await readFile(resolve(root, "rust-toolchain.toml"), "utf8");
  const { files, sha256: sourceClosure } = await import("../scripts/build-stream-native.mjs").then(module => module.sourceSnapshot());
  const candidates = linkerCandidates();
  const linkerPath = process.platform === "win32"
    ? candidates.find(value => /microsoft visual studio/i.test(value) && /hostx64[\\/]x64[\\/]link\.exe$/i.test(value)) ?? candidates.find(value => /microsoft visual studio/i.test(value))
    : candidates[0];
  return {
    schema: "acyclic.stream.native-build-inputs.research.v1",
    source: {
      revision: command("git", ["rev-parse", "HEAD"]),
      closure: sourceClosure,
      files,
    },
    target,
    path_strategy: {
      target_dir: resolve(targetDir),
      stable_target_dir_required_for_byte_identity: true,
      separate_target_directories_are_not_byte_identity_equivalent: true,
    },
    toolchain: {
      rustc: command("rustc", ["-Vv"]),
      cargo: command("cargo", ["-V"]),
      rustup_active_toolchain: command("rustup", ["show", "active-toolchain"]),
      node: command(process.execPath, ["--version"]),
      bun: command("bun", ["--version"]),
      linker: { path: linkerPath, version: linkerPath === undefined ? undefined : probe(linkerPath, ["/?"]) },
      napi_cli: packageVersion(packageJson, "@napi-rs/cli"),
    },
    configuration: {
      cargo_profile_release_incremental: profileIncremental(cargoToml),
      cargo_config_rustflags: configuredRustflags(cargoConfig),
      cargo_toml_sha256: `sha256:${createHash("sha256").update(cargoToml).digest("hex")}`,
      cargo_config_sha256: `sha256:${createHash("sha256").update(cargoConfig).digest("hex")}`,
      rust_toolchain_sha256: `sha256:${createHash("sha256").update(rustToolchain).digest("hex")}`,
      build_generator_sha256: await digest("scripts/build-stream-native.mjs"),
    },
    environment: Object.fromEntries(["CARGO_INCREMENTAL", "CARGO_PROFILE_RELEASE_INCREMENTAL", "CARGO_TARGET_DIR", "RUSTFLAGS", "RUSTC_WRAPPER", "CC", "CXX", "LINKER", "AR"].map(name => [name, process.env[name]]).filter(([, value]) => value !== undefined)),
    target_dir_exists: await exists(resolve(targetDir)),
  };
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  console.log(JSON.stringify(await build_inputs({ targetDir: process.argv[2] }), null, 2));
}
