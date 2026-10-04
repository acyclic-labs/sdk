#!/usr/bin/env node

// Compile a real TypeScript consumer against the generated package entry
// points. This checks the public SDK APIs and Rust-generated metadata without
// inventing React/Svelte adapters that are not part of this SDK worktree.
import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";

const args = process.argv.slice(2);
const value = (name) => { const i = args.indexOf(name); return i < 0 ? undefined : args[i + 1]; };
if (args.includes("--help") || !value("--packages-root") || !value("--output")) throw new Error("usage: qualify-typescript-generated-consumer.mjs --packages-root DIR --output FILE");
const packagesRoot = resolve(value("--packages-root"));
const outputPath = resolve(value("--output"));
const packages = {
  actors: "@acyclic-labs/actors",
  workers: "@acyclic-labs/workers",
  objects: "@acyclic-labs/objects",
  stream: "@acyclic-labs/stream",
  inference: "@acyclic-labs/inference",
  machines: "@acyclic-labs/machines",
  filesystem: "@acyclic-labs/fs",
};
const packagePaths = {
  [packages.actors]: join(packagesRoot, "actors", "dist", "index.d.ts"),
  [packages.workers]: join(packagesRoot, "workers", "dist", "index.d.ts"),
  [packages.objects]: join(packagesRoot, "objects", "dist", "index.d.ts"),
  [packages.stream]: join(packagesRoot, "stream", "dist", "index.d.ts"),
  [packages.inference]: join(packagesRoot, "inference", "dist", "index.d.ts"),
  [packages.machines]: join(packagesRoot, "machines", "dist", "index.d.ts"),
  [packages.filesystem]: join(packagesRoot, "filesystem", "dist", "browser.d.ts"),
  [`${packages.filesystem}/hosted`]: join(packagesRoot, "filesystem", "dist", "hosted.d.ts"),
};
for (const [name, path] of Object.entries(packagePaths)) if (!existsSync(path)) throw new Error(`missing generated consumer entry ${name}: ${path}`);
const fixtureRoot = mkdtempSync(join(tmpdir(), "acyclic-sdk-generated-consumer-"));
const sourcePath = join(fixtureRoot, "consumer.ts");
writeFileSync(sourcePath, `
import { fromEnv as actorsFromEnv } from "${packages.actors}";
import { fromEnv as workersFromEnv } from "${packages.workers}";
import { fromEnv as objectsFromEnv } from "${packages.objects}";
import { StreamClient } from "${packages.stream}";
import { fromEnv as inferenceFromEnv } from "${packages.inference}";
import { Machines } from "${packages.machines}";
import { openHostedFs } from "${packages.filesystem}/hosted";
import { ACTORS_REMOTE_POLICY } from "${packages.actors}/generated-client";
import { STREAM_REMOTE_POLICY } from "${packages.stream}/generated-client";

export async function generatedConsumerSmoke(endpoint: string, token: string): Promise<void> {
  const actors = await actorsFromEnv({ endpoint, token });
  const workers = await workersFromEnv({ endpoint, token });
  const objects = await objectsFromEnv({ endpoint, token });
  const stream = await StreamClient.fromEnv({ endpoint, token });
  const inference = inferenceFromEnv({ endpoint, token });
  const machines = Machines.fromEnv({ endpoint });
  const filesystem = openHostedFs({ endpoint, bearerToken: token });
  void [actors, workers, objects, stream, inference, machines, filesystem];
  if (ACTORS_REMOTE_POLICY.transport.native.length === 0 || STREAM_REMOTE_POLICY.transport.native.length === 0) throw new Error("Rust transport policy is empty");
}
`);
const configPath = join(fixtureRoot, "tsconfig.json");
writeFileSync(configPath, JSON.stringify({ compilerOptions: { strict: true, target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", noEmit: true, skipLibCheck: true, baseUrl: ".", paths: Object.fromEntries(Object.entries(packagePaths).map(([name, path]) => [name, [path]])) }, files: [sourcePath] }, null, 2));
const tsc = process.env.TSC ?? (process.platform === "win32" ? "C:/Users/varun/AppData/Roaming/npm/tsc.cmd" : "tsc");
const result = spawnSync(tsc, ["--project", configPath, "--pretty", "false"], { cwd: fixtureRoot, encoding: "utf8", shell: true, windowsHide: true });
if (result.status !== 0) throw new Error(`generated TypeScript consumer failed:\n${result.stdout || ""}${result.stderr || ""}`);
const receipt = { schema: "acyclic.sdk.typescript.generated-consumer-qualification.v1", status: "passed", packagesRoot, families: Object.keys(packages), entrypoints: Object.keys(packagePaths) };
writeFileSync(outputPath, `${JSON.stringify(receipt, null, 2)}\n`);
console.log(JSON.stringify({ schema: receipt.schema, status: receipt.status, output: outputPath, families: receipt.families.length }));
