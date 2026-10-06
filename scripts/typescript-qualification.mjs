#!/usr/bin/env node

// Rust owns package identity, archive digests, and receipt validation. This
// launcher keeps the existing release command shape; consumer execution stays
// a thin process harness because it invokes the selected package toolchain.
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [command, ...args] = process.argv.slice(2);
const cargo = process.env.CARGO ?? "cargo";
const cli = ["run", "--manifest-path", resolve(root, "rust/crates/sdk-generation/Cargo.toml"), "--locked", "--bin", "sdk-generation", "--", "typescript-qualification", "--source-root", root, "--output", resolve(root, "target/typescript-qualification-cli")];
if (command === "create" && args.length === 2) {
  cli.push("--qualification-action", "create", "--output", resolve(args[0]), "--source-sha", args[1]);
} else if (command === "verify" && args.length === 4) {
  cli.push("--qualification-action", "verify", "--receipt", resolve(args[0]), "--source-sha", args[1], "--asset", args[2], "--platform-receipt", resolve(args[3]));
} else if (command === "consumer" && args.length === 1) {
  const started = Date.now();
  const commands = [
    ["install", "--frozen-lockfile"],
    ["x", "tsc", "-b", "--force", "typescript/packages/sdk/tsconfig.json", "--pretty", "false"],
    ["x", "tsc", "-p", "typescript/packages/sdk/consumer-tsconfig.json", "--pretty", "false"],
    ["test", "typescript/packages/sdk/test/public-consumer.test.ts"],
  ];
  for (const commandArgs of commands) {
    const executable = process.platform === "win32" && /\.(?:cmd|bat)$/i.test(args[0]) ? "cmd" : args[0];
    const executableArgs = executable === "cmd" ? ["/d", "/c", args[0], ...commandArgs] : commandArgs;
    const result = spawnSync(executable, executableArgs, { cwd: root, encoding: "utf8", timeout: 90_000 });
    if (result.error || result.status !== 0) {
      console.log(JSON.stringify({ schema: 1, passed: false, elapsed_ms: Date.now() - started, error: String(result.error ?? `${result.stderr}${result.stdout}`).slice(-2000) }));
      process.exit(1);
    }
  }
  console.log(JSON.stringify({ schema: 1, passed: true, elapsed_ms: Date.now() - started, consumer: "typescript/packages/sdk/test/public-consumer.test.ts", surfaces: ["stream", "objects", "filesystem", "wasm"] }));
  process.exit(0);
} else {
  console.error("usage: typescript-qualification.mjs create OUTPUT SOURCE_SHA | verify RECEIPT SOURCE_SHA ASSET ARCHIVE | consumer BUN");
  process.exit(2);
}
const result = spawnSync(cargo, cli, { cwd: root, stdio: "inherit" });
if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
process.exit(result.status ?? 1);
