import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// Rust-owned contracts mirrored into TypeScript. Each module lists the Rust
// targets it reads as [package, kind, name] and renders their stdout into
// repository-relative files.
const modules = [
  "generate-runtime-routes.mjs",
  "filesystem-napi-types.mjs",
  "generate-filesystem-defaults.mjs",
  "generate-filesystem-hosted-contract.mjs",
  "generate-filesystem-git-compat-contract.mjs",
  "generate-inference-defaults.mjs",
  "generate-inference-terminal-metadata.mjs",
  "generate-inference-fixed-width-metadata.mjs",
  "generate-stream-http-contract.mjs",
  "generate-stream-token-operations.mjs",
  "generate-harness-limits-contract.mjs",
  "generate-harness-child-page-contract.mjs",
  "generate-harness-private-directory-page-contract.mjs",
  "generate-harness-conversation-page-contract.mjs",
  "generate-machines-managed-oci-contract.mjs",
  "generate-observe.mjs",
];

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const mode = process.argv[2] ?? "write";
if (mode !== "write" && mode !== "check") throw new Error("usage: generate-contracts.mjs write|check");
const generators = await Promise.all(modules.map(name => import(`./${name}`)));
const targets = generators.flatMap(generator => generator.rust ?? []);

// One build compiles every contract target from current Rust source; the
// emitted executables are then run directly instead of one `cargo run` each.
// Contracts are feature-independent, so every package builds without defaults.
const build = spawnSync(process.env.ACYCLIC_CARGO_BIN || "cargo", [
  "build", "--manifest-path", join(root, "Cargo.toml"), "--locked", "--quiet", "--no-default-features",
  "--message-format=json-render-diagnostics",
  ...[...new Set(targets.map(([name]) => name))].flatMap(name => ["-p", name]),
  ...targets.flatMap(([, kind, name]) => [`--${kind}`, name]),
], { cwd: root, encoding: "utf8", maxBuffer: 1 << 28, stdio: ["ignore", "pipe", "inherit"] });
if (build.status !== 0) throw new Error(`Rust contract build failed with status ${build.status ?? "unknown"}`);
const executables = new Map(build.stdout.split(/\r?\n/)
  .filter(line => line.startsWith("{"))
  .map(line => JSON.parse(line))
  .filter(message => message.reason === "compiler-artifact" && message.executable)
  .map(message => [message.target.name, message.executable]));
const run = name => {
  const executable = executables.get(name);
  if (!executable) throw new Error(`Rust contract target was not built: ${name}`);
  const result = spawnSync(executable, { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });
  if (result.status !== 0) throw new Error(`Rust contract target ${name} failed with status ${result.status ?? "unknown"}`);
  return result.stdout;
};

const stale = [];
for (const generator of generators) {
  const files = await generator.render((generator.rust ?? []).map(([, , name]) => run(name)), root);
  for (const [relative, content] of Object.entries(files)) {
    const path = join(root, relative);
    if (mode === "write") writeFileSync(path, content);
    else if (!existsSync(path) || !Buffer.from(content).equals(readFileSync(path))) stale.push(relative);
  }
}
if (stale.length > 0) throw new Error(`generated contracts are stale; run bun run generate:\n${stale.join("\n")}`);
