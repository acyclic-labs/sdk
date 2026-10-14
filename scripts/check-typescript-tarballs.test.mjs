import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { test } from "node:test";
import { probeSource, representativeExports } from "./check-typescript-tarballs.mjs";
import { validateArchive } from "./validate-npm-package.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const run = (command, args, cwd = root, env = process.env) => {
  const result = spawnSync(command, args, { cwd, env, encoding: "utf8", windowsHide: true });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, `${result.stdout ?? ""}${result.stderr ?? ""}`);
  return result.stdout;
};

test("root check:types executes Swarm's consumer regressions", async () => {
  const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  const commands = manifest.scripts["check:types"].split("&&").map(command => command.trim());
  const swarm = commands.find(command => command.includes("typescript/packages/swarm/tsconfig.type-tests.json"));
  assert.ok(swarm, "the root type gate must include Swarm");
  run("bun", ["x", ...swarm.split(/\s+/)]);
});

test("the release tarball checker exercises the installed Swarm API in Node and Bun", async () => {
  assert.equal(representativeExports["@acyclic-labs/swarm"], "SwarmClient");
  const tools = join(root, ".tools");
  await mkdir(tools, { recursive: true });
  const stage = await mkdtemp(join(tools, "swarm-tarball-smoke-"));
  try {
    const manifest = JSON.parse(await readFile(join(root, "typescript/packages/swarm/package.json"), "utf8"));
    const packs = join(stage, "packs");
    await mkdir(packs);
    const archive = run("bun", ["pm", "pack", "--destination", packs, "--ignore-scripts", "--quiet"], join(root, "typescript/packages/swarm")).trim();
    validateArchive(archive, "@acyclic-labs/swarm", manifest.version, "typescript/packages/swarm");
    await writeFile(join(stage, "package.json"), JSON.stringify({
      private: true, type: "module", workspaces: [],
      dependencies: { "@acyclic-labs/swarm": "file:" + archive.split(sep).join("/") },
    }));
    run("bun", ["install", "--ignore-scripts", "--no-progress"], stage, { ...process.env, BUN_INSTALL_CACHE_DIR: join(stage, "install-cache") });
    const installed = await realpath(join(stage, "node_modules/@acyclic-labs/swarm"));
    assert.ok(installed.startsWith(stage + sep), "smoke must use the packed package, not workspace sources");
    for (const runtime of ["node", "bun"]) {
      const probe = join(stage, `probe-${runtime}.mjs`);
      await writeFile(probe, probeSource(runtime, { "@acyclic-labs/swarm": "SwarmClient" }));
      assert.match(run(runtime, [probe], stage), /import and representative API checks passed for 1 packages/);
    }
    await writeFile(join(stage, "probe-types.ts"), `
import { SwarmClient, SwarmHttpError, type SwarmLimits } from "@acyclic-labs/swarm";
const limits: SwarmLimits = { width: 1, depth: 1, running: 1, web_calls: 1 };
const client = new SwarmClient({ endpoint: "https://swarms.example.test", token: "smoke" });
const error = new SwarmHttpError(503, { error: "unavailable" }, null);
void [limits, client, error];
`);
    await writeFile(join(stage, "tsconfig.json"), JSON.stringify({
      compilerOptions: { target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", lib: ["ES2022", "DOM"], strict: true, noEmit: true, types: [] },
      files: ["probe-types.ts"],
    }));
    run("bun", ["x", "tsc", "-p", join(stage, "tsconfig.json")]);
  } finally {
    assert.ok(resolve(stage).startsWith(resolve(tools) + sep));
    await rm(stage, { recursive: true, force: true });
  }
});
