import { createHash } from "node:crypto";
import { copyFile, cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { createRequire } from "node:module";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { nativeFamily } from "./native-family.mjs";
import { createNativeProducer } from "./build-native-family.mjs";
import { createNativeAssembler } from "./assemble-native-family.mjs";

const family = nativeFamily("filesystem");
const producer = createNativeProducer(family);
const assembler = createNativeAssembler(family);

const { version } = JSON.parse(await readFile(
  new URL("../typescript/packages/filesystem/package.json", import.meta.url), "utf8",
));
if (typeof version !== "string" || !/^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
  throw new Error("invalid filesystem package version");
}

const childBinding = process.env.ACYCLIC_FS_NAPI_CHILD_BINDING;
if (childBinding !== undefined) {
  if (process.env.ACYCLIC_FS_NAPI_CHILD_ADAPTER_ONLY !== "1") {
    await qualify(childBinding, process.env.ACYCLIC_FS_NAPI_CHILD_ROOT);
  }
  if (process.env.ACYCLIC_FS_NAPI_CHILD_ADAPTER === "1") {
    await qualifyAdapter(childBinding, process.env.ACYCLIC_FS_NAPI_CHILD_ROOT);
  }
  process.exit(0);
}

const adapterOnly = process.argv.includes("--adapter-only");
const adapter = adapterOnly || process.argv.includes("--adapter");
const args = process.argv.slice(2).filter((value) => value !== "--adapter" && value !== "--adapter-only");
if (args.length !== 4 || args[0] !== "--bundle" || args[2] !== "--producer-receipt"
    || !isAbsolute(args[1]) || !isAbsolute(args[3])) {
  throw new Error("usage: check-filesystem-napi.mjs --bundle ABSOLUTE_BUNDLE --producer-receipt ABSOLUTE_RECEIPT [--adapter | --adapter-only]");
}
const bundle = resolve(args[1]);
const { metadata } = await producer.assertBundle(bundle);
const receiptPath = resolve(args[3]);
assembler.assertNativeReceipt(JSON.parse(await readFile(receiptPath, "utf8")), metadata);
const inventory = adapter ? await assembler.sourceNativeInventory(metadata.source_revision) : undefined;
const companion = inventory?.companions.find((item) => item.selected_target === metadata.selected_target);
if (adapter && companion === undefined) throw new Error("native adapter companion target is absent");
if (adapter && companion.main !== metadata.artifact.path.slice("generated/native/".length)) {
  throw new Error("native adapter artifact filename differs from maintained target mapping");
}
const temporary = await mkdtemp(join(tmpdir(), "acyclic-fs-napi-"));
const privateBundle = join(temporary, "bundle");
const bindingPath = join(privateBundle, metadata.artifact.path.slice("generated/native/".length));
try {
  await cp(bundle, privateBundle, { recursive: true });
  await producer.assertBundle(privateBundle);
  await copyFile(receiptPath, join(temporary, "producer-receipt.json"));
  await /** @type {Promise<void>} */ (new Promise((resolveChild, rejectChild) => {
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url)], {
      env: {
        ...process.env,
        ACYCLIC_FS_NAPI_CHILD_BINDING: bindingPath,
        ACYCLIC_FS_NAPI_CHILD_ROOT: join(temporary, "engine"),
        ACYCLIC_FS_NAPI_CHILD_ADAPTER: adapter ? "1" : "0",
        ACYCLIC_FS_NAPI_CHILD_ADAPTER_ONLY: adapterOnly ? "1" : "0",
        ACYCLIC_FS_NAPI_CHILD_BUNDLE: privateBundle,
        ACYCLIC_FS_NAPI_CHILD_COMPANION: companion === undefined ? "" : JSON.stringify(companion.manifest),
        ACYCLIC_FS_NAPI_CHILD_RECEIPT: join(temporary, "producer-receipt.json"),
        ACYCLIC_FS_NAPI_CHILD_OUTPUT: resolve(bundle, "..", "qualification"),
      },
      stdio: "inherit",
    });
    child.once("error", rejectChild);
    child.once("exit", (code, signal) => {
      if (code === 0) resolveChild();
      else rejectChild(new Error(`N-API child exited with code ${code} and signal ${signal}`));
    });
  }));
} finally {
  await rm(temporary, { recursive: true, force: true });
}

async function qualify(bindingPath, engineRoot) {
  if (engineRoot === undefined) throw new Error("N-API child root is absent");
  const binding = createRequire(import.meta.url)(bindingPath);
  const capabilities = binding.nativeCapabilities();
  if (capabilities.version !== version) {
    throw new Error("N-API capability version does not match the package");
  }
  const fs = await binding.NativeFs.open(engineRoot, {
    maximumEntries: 64,
    maximumBytes: 1024n * 1024n,
    maximumInFlight: 8,
    maximumWaitersPerObject: 8,
  });
  const workspace = await fs.createWorkspace("qualification");
  const before = await workspace.sync();
  const committed = await workspace.write("/abi.txt", Buffer.from("napi"));
  if (committed.status !== "committed") {
    throw new Error(`N-API write returned ${committed.status}`);
  }
  const bytes = await workspace.read("/abi.txt", 16n);
  if (!Buffer.from(bytes).equals(Buffer.from("napi"))) {
    throw new Error("N-API read did not preserve bytes");
  }
  const after = await workspace.sync();
  const changes = await workspace.diff(before, after, 32);
  if (changes.changes().files.length === 0) {
    throw new Error("N-API change set omitted the authored file");
  }
  fs.cancel();
  console.log(`acyclic-fs N-API ABI passed on ${process.platform}-${process.arch}`);
}

async function qualifyAdapter(bindingPath, engineRoot) {
  if (engineRoot === undefined) throw new Error("N-API adapter root is absent");
  // Pack maintained manifests and exact privately retained bytes, then install
  // real archives before exercising the public createRequire loader.
  const installationRoot = join(engineRoot, "installed");
  const packageRoot = join(installationRoot, "node_modules", "@acyclic-labs", "fs");
  const parentRoot = join(engineRoot, "parent");
  const bundle = process.env.ACYCLIC_FS_NAPI_CHILD_BUNDLE;
  const manifest = JSON.parse(process.env.ACYCLIC_FS_NAPI_CHILD_COMPANION ?? "");
  const output = process.env.ACYCLIC_FS_NAPI_CHILD_OUTPUT;
  const receipt = process.env.ACYCLIC_FS_NAPI_CHILD_RECEIPT;
  if (!bundle || !output || !receipt || manifest.version !== version || manifest.private !== false
      || typeof manifest.main !== "string" || !manifest.main.endsWith(".node")) {
    throw new Error("native adapter requires the maintained manifest and original compiler receipt");
  }
  const companionRoot = join(engineRoot, "companion");
  await mkdir(parentRoot, { recursive: true });
  await cp(new URL("../typescript/packages/filesystem/dist", import.meta.url), join(parentRoot, "dist"), { recursive: true });
  const generatedRoot = fileURLToPath(new URL("../typescript/packages/filesystem/generated", import.meta.url));
  await cp(generatedRoot, join(parentRoot, "generated"), {
    recursive: true, filter: source => source !== join(generatedRoot, "native"),
  });
  await mkdir(join(parentRoot, "generated", "native"), { recursive: true });
  for (const name of ["binding.cjs", "binding.d.ts", "native-targets.json"]) {
    await copyFile(join(bundle, name), join(parentRoot, "generated", "native", name));
  }
  await copyFile(new URL("../typescript/packages/filesystem/package.json", import.meta.url), join(parentRoot, "package.json"));
  await mkdir(companionRoot, { recursive: true });
  await copyFile(bindingPath, join(companionRoot, manifest.main));
  for (const name of ["native-targets.json", "generation-manifest.json"]) {
    await copyFile(join(bundle, name), join(companionRoot, name));
  }
  await copyFile(receipt, join(companionRoot, "producer-receipt.json"));
  await writeFile(join(companionRoot, "package.json"), JSON.stringify(manifest));
  const parentArchive = join(output, await assembler.packArchive(parentRoot, output));
  const companionArchive = join(output, await assembler.packArchive(companionRoot, output));
  await mkdir(installationRoot, { recursive: true });
  await writeFile(join(installationRoot, "package.json"), JSON.stringify({
    private: true,
    dependencies: { "@acyclic-labs/fs": `file:${parentArchive}`, [manifest.name]: `file:${companionArchive}` },
  }));
  const bun = process.versions.bun === undefined ? (process.env.ACYCLIC_FS_NAPI_BUN ?? "bun") : process.execPath;
  const installed = spawnSync(bun, ["install", "--production", "--ignore-scripts", "--no-save", "--no-progress", `--cpu=${process.arch}`, `--os=${process.platform}`], {
    cwd: installationRoot, stdio: "inherit",
  });
  if (installed.error) throw installed.error;
  if (installed.status !== 0) throw new Error("native adapter archive installation failed");
  const installedBinding = createRequire(pathToFileURL(join(packageRoot, "dist", "native.js")))(manifest.name);
  if (installedBinding.nativeCapabilities().version !== version) throw new Error("installed archive ABI differs");
  const metadata = JSON.parse(await readFile(join(bundle, "native-targets.json"), "utf8"));
  const proof = {
    schema: "acyclic.filesystem.native-runtime-qualification.v1",
    source_commit: metadata.source_revision, source_sha256: metadata.source_sha256, target: metadata.selected_target,
    platform: process.platform, arch: process.arch, node: process.version,
    artifact: metadata.artifact,
    producer_receipt_sha256: `sha256:${createHash("sha256").update(await readFile(receipt)).digest("hex")}`,
    archives: await Promise.all([parentArchive, companionArchive].map(async path => ({
      path: path.split(/[\\/]/u).at(-1), sha256: `sha256:${createHash("sha256").update(await readFile(path)).digest("hex")}`,
    }))),
  };
  const { openNativeFs, openNativeWorkspaceGraph, DEFAULT_OBJECT_CACHE_OPTIONS, portableVolumeOptions } = await import(pathToFileURL(join(packageRoot, "dist", "native.js")).href);
  const engine = await openNativeFs({
    root: join(engineRoot, "public-adapter"),
    objectCache: {
      ...DEFAULT_OBJECT_CACHE_OPTIONS,
    },
  });
  try {
    const { exerciseWorkspace } = await import("../typescript/packages/filesystem/test/workspace-composition.mjs");
    await exerciseWorkspace(engine);
    const workspace = await engine.createWorkspace("qualification");
    const originalId = workspace.id;
    if (originalId.constructor !== Uint8Array) throw new Error("native adapter exposed a Buffer workspace identity");
    originalId.fill(0);
    if (workspace.id.every((byte) => byte === 0)) throw new Error("native adapter exposed a mutable workspace identity view");
    const before = await workspace.sync();
    await workspace.write("/adapter.txt", new Uint8Array([1, 2, 3]));
    const after = await workspace.sync();
    const changes = await workspace.diff(before, after, 32);
    if (changes.changes().files.length === 0) throw new Error("native adapter lost the workspace change set");
    const pinned = await after.pin("qualification-generation");
    if (!pinned.id.every((byte, index) => byte === after.id[index])) {
      throw new Error("native adapter changed the pinned generation identity");
    }
    await workspace.write("/second.txt", new Uint8Array([4]));
    const latest = await workspace.sync();
    const secondChange = await workspace.diff(after, latest, 32);
    const composed = await changes.compose(secondChange, 32);
    if (
      !composed.from.id.every((byte, index) => byte === before.id[index])
      || !composed.to.id.every((byte, index) => byte === latest.id[index])
      || composed.changes().files.length === 0
    ) {
      throw new Error("native adapter lost composed change-set endpoints");
    }
    const foreignEngine = await openNativeFs({
      root: join(engineRoot, "foreign-adapter"),
      objectCache: {
        ...DEFAULT_OBJECT_CACHE_OPTIONS,
      },
    });
    try {
      const foreignWorkspace = await foreignEngine.createWorkspace("foreign");
      const foreignGeneration = await foreignWorkspace.sync();
      const foreignChange = await foreignWorkspace.diff(foreignGeneration, foreignGeneration, 32);
      const forkAtChild = await workspace.forkAt("fork-at-child", before);
      const forkAtChildGeneration = await forkAtChild.sync();
      await forkAtChild.forkAt("fork-at-grandchild", forkAtChildGeneration);
      const rejectsForeign = async (operation, kind) => {
        try { await operation(); }
        catch (error) {
          if (error instanceof TypeError && error.message === `${kind} belongs to another filesystem engine`) return;
          throw error;
        }
        throw new Error(`native adapter accepted a foreign ${kind}`);
      };
      await rejectsForeign(() => workspace.forkAt("foreign-fork", foreignGeneration), "generation");
      await rejectsForeign(() => forkAtChild.forkAt("foreign-fork-at-child", foreignGeneration), "generation");
      await rejectsForeign(() => workspace.diff(before, foreignGeneration, 32), "generation");
      await rejectsForeign(() => workspace.joinInto(foreignWorkspace, {
        history: "merge", maximumGenerations: 32, maximumChanges: 32, maximumConflicts: 32,
      }), "workspace");
      await rejectsForeign(() => changes.compose(foreignChange, 32), "change set");
      const graph = await openNativeWorkspaceGraph(join(engineRoot, "graph-adapter"));
      await graph.registerRoot(workspace);
      const graphChild = await graph.fork(workspace, "graph-child");
      const graphChildGeneration = await graphChild.sync();
      await graphChild.forkAt("graph-grandchild", graphChildGeneration);
      await rejectsForeign(() => graphChild.forkAt("foreign-graph-fork", foreignGeneration), "generation");
    } finally {
      foreignEngine.close();
    }

    const fork = await workspace.fork("conflict-fork");
    await workspace.write("/adapter.txt", new Uint8Array([4]));
    await fork.write("/adapter.txt", new Uint8Array([5]));
    const joinPlan = await fork.joinInto(workspace, {
      history: "merge", maximumGenerations: 64, maximumChanges: 64, maximumConflicts: 16,
    });
    const joinResult = await joinPlan.apply(joinPlan.targetHead);
    const fileConflict = joinResult.conflicts.find((conflict) => conflict.kind === "file");
    if (joinResult.status !== "conflicted" || fileConflict?.fileId.constructor !== Uint8Array) {
      throw new Error("native adapter lost typed file merge conflicts");
    }

    const volume = await engine.createVolume(portableVolumeOptions("ephemeral"));
    const checkout = await volume.checkout({ access: "read-write", consistency: "pinned", mutationMode: "private-cow" });
    await checkout.createFile("/adapter.bin", new Uint8Array([4, 5]));
    const batch = await checkout.lookupBatchNoFollow(["/adapter.bin", "/missing"]);
    if (batch.entries[0]?.fileId?.constructor !== Uint8Array || batch.entries[1]?.fileId !== undefined) {
      throw new Error("native adapter lost typed lookup positions");
    }
    const read = await checkout.readFileRange("/adapter.bin", 0n, 2n);
    if (read.bytes.constructor !== Uint8Array || read.bytes[0] !== 4 || read.bytes[1] !== 5) {
      throw new Error("native adapter lost typed file bytes");
    }
    const readWorkKeys = Object.keys(read.work).sort();
    const acquisitionWorkKeys = Object.keys(checkout.acquisitionWork).sort();
    if (
      readWorkKeys.length === 0
      || JSON.stringify(readWorkKeys) !== JSON.stringify(acquisitionWorkKeys)
      || Object.values(read.work).some((value) => typeof value !== "bigint")
      || Object.values(checkout.acquisitionWork).some((value) => typeof value !== "bigint")
    ) {
      throw new Error("native adapter lost the generated work-counter object");
    }
    const transactionBytes = new Uint8Array([9]);
    const transactionPromise = checkout.applyTransaction([{
      kind: "create-file", path: "/adapter-transaction.bin", bytes: transactionBytes,
    }]);
    transactionBytes[0] = 7;
    await transactionPromise;
    const transactionRead = await checkout.readFileRange("/adapter-transaction.bin", 0n, 1n);
    if (transactionRead.bytes[0] !== 9) {
      throw new Error("native adapter transaction observed a mutated caller buffer");
    }
  } finally {
    engine.close();
  }
  const nodeConsumer = spawnSync(process.env.ACYCLIC_FS_NAPI_NODE ?? (process.versions.bun === undefined ? process.execPath : "node"), [
    fileURLToPath(new URL("../typescript/packages/filesystem/test/native-public-installed.mjs", import.meta.url)),
    packageRoot,
  ], { stdio: "inherit" });
  if (nodeConsumer.error) throw nodeConsumer.error;
  if (nodeConsumer.status !== 0) throw new Error("installed native Node.js consumer qualification failed");
  await writeFile(join(output, "runtime-qualification.json"), `${JSON.stringify(proof, null, 2)}\n`);
  console.log(`acyclic-fs native TypeScript adapter passed on ${process.platform}-${process.arch}`);
}
