import { mkdtemp, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packagesRoot = join(root, "typescript", "packages");
const releasePackages = JSON.parse(await readFile(join(root, "release", "npm-packages.json"), "utf8"));
const publishedPackageEntries = releasePackages.filter(item => item.source === "typescript");
const workspacePackageEntries = [];
for (const entry of await readdir(packagesRoot, { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  try {
    const manifest = JSON.parse(await readFile(join(packagesRoot, entry.name, "package.json"), "utf8"));
    if (manifest.private === false) workspacePackageEntries.push({ directory: entry.name, name: manifest.name });
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
}
workspacePackageEntries.sort((left, right) => left.name.localeCompare(right.name));
for (const entry of publishedPackageEntries) {
  if (!workspacePackageEntries.some(candidate => candidate.name === entry.name && candidate.directory === entry.directory)) {
    throw new Error(`published package is absent from the workspace: ${entry.name}`);
  }
}
const packageEntries = workspacePackageEntries;
const packageDirectories = packageEntries.map(item => item.directory);

const expectedExports = {
  "@acyclic-labs/fs": "openBrowserFs",
  "@acyclic-labs/harness": "Harness",
  "@acyclic-labs/inference": "InferenceClient",
  "@acyclic-labs/machines": "machineId",
  "@acyclic-labs/objects": "idempotencyKey",
  "@acyclic-labs/pi": "piProvider",
  "@acyclic-labs/sdk": "harness",
  "@acyclic-labs/stream": "MemoryStreamProvider",
};
for (const entry of packageEntries) {
  if (!(entry.name in expectedExports)) {
    throw new Error(`missing tarball smoke contract for ${entry.name}`);
  }
}
if (Object.keys(expectedExports).length !== packageEntries.length) {
  throw new Error("tarball smoke contracts do not match the published TypeScript package set");
}

const run = (command, args, options = {}) => {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? root,
    encoding: "utf8",
    stdio: options.capture === false ? "inherit" : ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    const output = `${result.stdout ?? ""}${result.stderr ?? ""}`.trim();
    throw new Error(`${command} ${args.join(" ")} exited ${result.status}${output ? `: ${output}` : ""}`);
  }
  return result.stdout ?? "";
};

const packageJson = async (directory) =>
  JSON.parse(await readFile(join(packagesRoot, directory, "package.json"), "utf8"));

const probeSource = (runtime, names) => `
const expected = ${JSON.stringify(names)};
for (const [name, exportName] of Object.entries(expected)) {
  const module = await import(name);
  if (!(exportName in module)) throw new Error(name + " is missing representative export " + exportName);
  if (typeof module[exportName] !== "function" && name !== "@acyclic-labs/sdk") {
    throw new Error(name + " representative export " + exportName + " is not callable");
  }
}
const checks = {
  "@acyclic-labs/fs": (m) => typeof m.openBrowserFs === "function",
  "@acyclic-labs/harness": (m) => typeof m.Harness.builder === "function",
  "@acyclic-labs/inference": (m) => typeof m.InferenceClient === "function",
  "@acyclic-labs/machines": async (m) => {
    const provider = new m.SimulatedMachines();
    const created = await provider.create({
      idempotencyKey: m.idempotencyKey("tarball-smoke"),
      image: { kind: "custom", digestHex: "07".repeat(32) },
      compatibility: { kind: "best-effort" }, performance: "elastic",
      suspension: { kind: "manual" }, expiration: { kind: "never" },
      networkPolicyDigestHex: "08".repeat(32), budgets: { spendMicros: 0n, concurrency: 0 },
    });
    return created.kind === "created" && (await provider.inspectMachine(created.machine.id)).state === "running";
  },
  "@acyclic-labs/objects": (m) => m.idempotencyKey("tarball-smoke") === "tarball-smoke",
  "@acyclic-labs/pi": (m) => typeof m.piProvider === "function",
  "@acyclic-labs/sdk": (m) => typeof m.harness === "object" && typeof m.machines === "object",
  "@acyclic-labs/stream": async (m) => {
    const provider = new m.MemoryStreamProvider();
    const appended = await provider.append("tarball/smoke", [new Uint8Array([7])]);
    return appended.ok && appended.tail === 1n && await provider.tail("tarball/smoke") === 1n;
  },
};
for (const [name, check] of Object.entries(checks)) {
  const module = await import(name);
  if (!(await check(module))) throw new Error(name + " representative API check failed");
}
console.log(${JSON.stringify(runtime)} + " import and representative API checks passed for " + Object.keys(expected).length + " packages");
`;

const main = async () => {
  const tempBase = resolve(tmpdir());
  const tempRoot = await mkdtemp(join(tempBase, "acyclic-sdk-tarball-smoke-"));
  const packDirectory = join(tempRoot, "packs");
  const tarballs = new Map();
  try {
    await mkdir(packDirectory, { recursive: true });
    for (const entry of publishedPackageEntries) {
      const manifest = await packageJson(entry.directory);
      if (manifest.name !== entry.name || manifest.private !== false) {
        throw new Error(`published release identity differs for ${entry.directory}`);
      }
    }
    const manifests = await Promise.all(packageDirectories.map(packageJson));
    for (let i = 0; i < manifests.length; i += 1) {
      const manifest = manifests[i];
      if (manifest.name !== packageEntries[i].name) {
        throw new Error(`release identity differs for ${packageDirectories[i]}`);
      }
      const packageDirectory = join(packagesRoot, packageDirectories[i]);
      const packedPath = run("bun", ["pm", "pack", "--destination", packDirectory, "--ignore-scripts", "--quiet"], { cwd: packageDirectory }).trim();
      if (!packedPath) throw new Error(`${manifest.name} did not produce a tarball path`);
      tarballs.set(manifest.name, resolve(packedPath));
    }
    const tarballSpec = (file) => `file:./${relative(tempRoot, file).split(sep).join("/")}`;
    const dependencies = Object.fromEntries([...tarballs].map(([name, file]) => [name, tarballSpec(file)]));
    const peerDependencies = Object.fromEntries(manifests.flatMap((manifest) => Object.entries(manifest.peerDependencies ?? {}).filter(([name]) => !tarballs.has(name))));
    await writeFile(join(tempRoot, "package.json"), JSON.stringify({
      private: true,
      type: "module",
      workspaces: [],
      dependencies: { ...dependencies, ...peerDependencies },
      overrides: Object.fromEntries([...tarballs].map(([name, file]) => [name, tarballSpec(file)])),
    }, null, 2));
    await writeFile(join(tempRoot, "probe.mjs"), probeSource("Bun", expectedExports));
    run("bun", ["install", "--no-progress"], { cwd: tempRoot });
    run("bun", ["probe.mjs"], { cwd: tempRoot });
    await writeFile(join(tempRoot, "probe-node.mjs"), probeSource("Node", expectedExports));
    run("node", ["probe-node.mjs"], { cwd: tempRoot });
    console.log(`TypeScript tarball smoke passed: ${tarballs.size} workspace packages (${publishedPackageEntries.length} published, ${tarballs.size - publishedPackageEntries.length} workspace-only); Bun and Node imports verified`);
  } finally {
    const prefix = `${tempBase}${sep}acyclic-sdk-tarball-smoke-`;
    if (!resolve(tempRoot).startsWith(prefix)) throw new Error("refusing to remove an unexpected tarball smoke directory");
    await rm(tempRoot, { recursive: true, force: true });
  }
};

if (import.meta.main) {
  await main();
}
