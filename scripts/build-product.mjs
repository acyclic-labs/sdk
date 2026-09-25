import { spawnSync } from "node:child_process";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const target = resolve(root, process.env.CARGO_TARGET_DIR ?? "target");
const cargoHome = resolve(process.env.CARGO_HOME ?? join(homedir(), ".cargo"));

if (process.env.RUSTFLAGS || process.env.CARGO_ENCODED_RUSTFLAGS) {
  throw new Error("build-product owns Rust flags so its output stays reproducible");
}

const flags = [
  "--remap-path-prefix", `${root}=.`,
  "--remap-path-prefix", `${target}=/cargo/build-dir`,
  "--remap-path-prefix", `${cargoHome}=/cargo/home`,
];
if (process.platform === "win32") flags.push("-C", "link-arg=/Brepro");

const cargoArguments = ["build", "--locked", "--release", "-p", "acyclic-plugin"];
if (process.env.CARGO_BUILD_TARGET) cargoArguments.push("--target", process.env.CARGO_BUILD_TARGET);

const result = spawnSync(
  process.env.CARGO ?? "cargo",
  cargoArguments,
  {
    cwd: root,
    env: {
      ...process.env,
      CARGO_TARGET_DIR: target,
      CARGO_ENCODED_RUSTFLAGS: flags.join("\x1f"),
    },
    stdio: "inherit",
  },
);
if (result.error) throw result.error;
process.exit(result.status ?? 1);
