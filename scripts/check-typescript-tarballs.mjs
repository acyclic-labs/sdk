import { mkdtemp, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
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
const rootManifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
const rootTsconfig = JSON.parse(await readFile(join(root, "tsconfig.json"), "utf8"));
const checkedProjects = new Set(rootTsconfig.references.map(reference =>
  relative(packagesRoot, resolve(root, reference.path)).split(sep).join("/")));
for (const entry of packageEntries) {
  if (!checkedProjects.has(entry.directory)) {
    throw new Error(`TypeScript workspace package is absent from the root type check: ${entry.name}`);
  }
}

const expectedExports = {
  "@acyclic-labs/actors": "HttpActorsClient",
  "@acyclic-labs/fs": "openBrowserFs",
  "@acyclic-labs/harness": "Harness",
  "@acyclic-labs/inference": "InferenceClient",
  "@acyclic-labs/machines": "machineId",
  "@acyclic-labs/objects": "idempotencyKey",
  "@acyclic-labs/pi": "piProvider",
  "@acyclic-labs/sdk": "harness",
  "@acyclic-labs/stream": "MemoryStreamProvider",
  "@acyclic-labs/workers": "HttpWorkersClient",
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

const hasExportedFiles = (manifest, packageDirectory) => {
  const targets = [];
  const collect = (value) => {
    if (typeof value === "string") targets.push(value);
    else if (value && typeof value === "object") {
      for (const child of Object.values(value)) collect(child);
    }
  };
  collect(manifest.exports);
  return targets.length > 0 && targets.every((target) =>
    target.startsWith("./") && existsSync(join(packageDirectory, target.slice(2))));
};

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
  "@acyclic-labs/actors": (m) => typeof m.HttpActorsClient === "function" && typeof m.CreateActorRequestSchema === "object",
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
  "@acyclic-labs/workers": (m) => typeof m.HttpWorkersClient === "function" && typeof m.SubmitJobRequestSchema === "object",
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
      if (!hasExportedFiles(manifest, packageDirectory)) {
        run("bun", ["run", "build"], { cwd: packageDirectory });
      }
      if (!hasExportedFiles(manifest, packageDirectory)) {
        run("bun", ["x", "tsc", "-b", "--force"], { cwd: packageDirectory });
      }
      if (!hasExportedFiles(manifest, packageDirectory)) {
        throw new Error(`${manifest.name} build did not produce its exported files`);
      }
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
      dependencies: { ...dependencies, ...peerDependencies, "@types/node": rootManifest.devDependencies["@types/node"] },
      overrides: Object.fromEntries([...tarballs].map(([name, file]) => [name, tarballSpec(file)])),
    }, null, 2));
    await writeFile(join(tempRoot, "probe.mjs"), probeSource("Bun", expectedExports));
    run("bun", ["install", "--no-progress"], { cwd: tempRoot });
    run("bun", ["probe.mjs"], { cwd: tempRoot });
    await writeFile(join(tempRoot, "probe-node.mjs"), probeSource("Node", expectedExports));
    run("node", ["probe-node.mjs"], { cwd: tempRoot });
    const typeImports = Object.entries(expectedExports).map(([name, exportName], index) =>
      `import { ${exportName} as package${index} } from ${JSON.stringify(name)};`);
    await writeFile(join(tempRoot, "probe-types.ts"), `${typeImports.join("\n")}\nvoid [${typeImports.map((_, index) => `package${index}`).join(", ")}];\n`);
    await writeFile(join(tempRoot, "tsconfig.types.json"), JSON.stringify({
      compilerOptions: {
        target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext",
        lib: ["ESNext", "DOM", "DOM.Iterable"], strict: true, noEmit: true,
        skipLibCheck: false, types: ["node"],
      },
      files: ["probe-types.ts"],
    }, null, 2));
    run("node", [join(root, "node_modules", "typescript", "bin", "tsc"), "-p", "tsconfig.types.json"], { cwd: tempRoot });
    console.log(`TypeScript tarball smoke passed: ${tarballs.size} workspace packages (${publishedPackageEntries.length} published, ${tarballs.size - publishedPackageEntries.length} workspace-only); Bun and Node imports and strict declarations verified`);
  } finally {
    const prefix = `${tempBase}${sep}acyclic-sdk-tarball-smoke-`;
    if (!resolve(tempRoot).startsWith(prefix)) throw new Error("refusing to remove an unexpected tarball smoke directory");
    await rm(tempRoot, { recursive: true, force: true });
  }
};

if (import.meta.main) {
  await main();
}
