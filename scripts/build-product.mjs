import { execFileSync, spawnSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { sha256File } from "./file-digest.mjs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

async function buildProduct() {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const target = resolve(root, process.env.CARGO_TARGET_DIR ?? "target");
  const cargoHome = resolve(process.env.CARGO_HOME ?? join(homedir(), ".cargo"));
  const source = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
  const dirty = execFileSync("git", ["status", "--porcelain", "--untracked-files=normal"], { cwd: root, encoding: "utf8" }).length !== 0;

  if (process.env.RUSTFLAGS || process.env.CARGO_ENCODED_RUSTFLAGS) {
    throw new Error("build-product flags refused; recovery: env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS node scripts/build-product.mjs");
  }

  const flags = [
    "--remap-path-prefix", `${root}=.`,
    "--remap-path-prefix", `${target}=/cargo/build-dir`,
    "--remap-path-prefix", `${cargoHome}=/cargo/home`,
  ];
  if (process.platform === "win32") flags.push("-C", "link-arg=/Brepro");

  // Configured flags join the repository's own target flags in
  // .cargo/config.toml, which RUSTFLAGS would replace.
  const cargoArguments = [
    "build", "--locked", "--profile", "dist", "-p", "acyclic-plugin", "-p", "graphcoder", "--timings",
    "--config", `target.'cfg(all())'.rustflags = ${JSON.stringify(flags)}`,
  ];
  if (process.env.CARGO_BUILD_TARGET) cargoArguments.push("--target", process.env.CARGO_BUILD_TARGET);

  const result = spawnSync(
    process.env.CARGO ?? "cargo",
    cargoArguments,
    {
      cwd: root,
      env: {
        ...process.env,
        CARGO_TARGET_DIR: target,
        // Cargo's intermediate objects must share the selected output lifetime.
        CARGO_BUILD_BUILD_DIR: join(target, "build"),
      },
      stdio: "inherit",
    },
  );
  if (result.error || result.status !== 0) {
    throw new Error(`build-product cargo step failed (${result.error?.message ?? result.status ?? result.signal}); recovery: node scripts/build-product.mjs`);
  }
  const output = join(target, ...(process.env.CARGO_BUILD_TARGET ? [process.env.CARGO_BUILD_TARGET] : []), "dist");
  const executables = ["acyclic", "graphcoder"].map(name => name + (process.env.CARGO_BUILD_TARGET?.includes("windows") || (!process.env.CARGO_BUILD_TARGET && process.platform === "win32") ? ".exe" : ""));
  const hashes = Object.fromEntries(await Promise.all(executables.map(async name => [name, await sha256File(join(output, name))])));
  writeFileSync(join(output, "PRODUCT-BUILD.json"), JSON.stringify({ format: 1, source, dirty, hashes }) + "\n");

}
try { await buildProduct(); }
catch (error) { throw new Error(`product build failed: ${error.message}; recovery: node scripts/build-product.mjs`, { cause: error }); }
