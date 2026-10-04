import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createReadStream, existsSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const libraryNames = {
  win32: "acyclic_fs_napi.dll",
  linux: "libacyclic_fs_napi.so",
  darwin: "libacyclic_fs_napi.dylib",
};
const libraryName = libraryNames[process.platform];
if (libraryName === undefined) {
  throw new Error(`unsupported N-API qualification platform ${process.platform}`);
}
const { version } = JSON.parse(await readFile(
  new URL("../typescript/packages/filesystem/package.json", import.meta.url), "utf8",
));
if (typeof version !== "string" || !/^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
  throw new Error("invalid filesystem package version");
}

const childBinding = process.env.ACYCLIC_FS_NAPI_CHILD_BINDING;
if (childBinding !== undefined) {
  await qualify(childBinding, process.env.ACYCLIC_FS_NAPI_CHILD_ROOT);
  if (process.env.ACYCLIC_FS_NAPI_CHILD_ADAPTER === "1") {
    await qualifyAdapter(childBinding, process.env.ACYCLIC_FS_NAPI_CHILD_ROOT);
  }
  process.exit(0);
}

const adapter = process.argv.includes("--adapter");
const positional = process.argv.slice(2).filter((value) => value !== "--adapter");
const output = positional[0];
if (positional.length > 1 || (output !== undefined && !isAbsolute(output))) {
  throw new Error("usage: check-filesystem-napi.mjs [--adapter] [ABSOLUTE_OUTPUT]");
}

const targetRoot = resolve(process.env.CARGO_TARGET_DIR ?? "target");
const profile = process.env.ACYCLIC_FS_NAPI_PROFILE ?? "debug";
const source = join(targetRoot, profile, libraryName);
const temporary = await mkdtemp(join(tmpdir(), "acyclic-fs-napi-"));
const bindingPath = join(temporary, `${basename(libraryName)}.node`);
try {
  await copyFile(source, bindingPath);
  await new Promise((resolveChild, rejectChild) => {
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url)], {
      env: {
        ...process.env,
        ACYCLIC_FS_NAPI_CHILD_BINDING: bindingPath,
        ACYCLIC_FS_NAPI_CHILD_ROOT: join(temporary, "engine"),
        ACYCLIC_FS_NAPI_CHILD_ADAPTER: adapter ? "1" : "0",
      },
      stdio: "inherit",
    });
    child.once("error", rejectChild);
    child.once("exit", (code, signal) => {
      if (code === 0) resolveChild();
      else rejectChild(new Error(`N-API child exited with code ${code} and signal ${signal}`));
    });
  });
  if (output !== undefined) {
    // Retain the private copy loaded by the successful child, never recopy the build output.
    await mkdir(dirname(output), { recursive: true });
    await mkdir(output);
    const name = `acyclic-fs-${version}-${process.platform}-${process.arch}.node`;
    await copyFile(bindingPath, join(output, name));
    const digest = createHash("sha256");
    for await (const chunk of createReadStream(join(output, name))) digest.update(chunk);
    await writeFile(join(output, "SHA256SUMS"), `${digest.digest("hex")}  ${name}\n`, { flag: "wx" });
  }
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
  await qualifyProcessOwner(binding);
  console.log(`acyclic-fs N-API ABI passed on ${process.platform}-${process.arch}`);
}

async function qualifyProcessOwner(binding) {
  if (typeof binding.NativeProcessOwner !== "function") {
    throw new Error("N-API companion did not export NativeProcessOwner");
  }
  const owner = new binding.NativeProcessOwner();
  const executable = process.platform === "win32" ? "node" : process.execPath;
  const directory = await mkdtemp(join(tmpdir(), "acyclic-native-owner-"));
  const pidFile = join(directory, "grandchild.pid");
  const rootExitFile = join(directory, "root-exit");
  const grandchild = "const fs=require('node:fs'); fs.writeFileSync(process.argv[1], String(process.pid)); setInterval(() => {}, 100000);";
  const root = `const fs=require('node:fs'); const {spawn}=require('node:child_process'); spawn(process.execPath, ['-e', ${JSON.stringify(grandchild)}, process.argv[1]], { detached: true, windowsHide: true, stdio: 'ignore', env: {} }); fs.writeFileSync(process.argv[2], 'exited');`;
  try {
    const environment = [
      `PATH=${process.env.PATH ?? ""}`,
      ...(process.platform === "win32" ? [`SystemRoot=${process.env.SystemRoot ?? ""}`] : []),
    ];
    await qualifyProcessIo(owner, executable, environment);
    const spawned = owner.spawn(executable, ["-e", root, pidFile, rootExitFile], null, environment);
    const deadline = Date.now() + 5_000;
    while (!exists(pidFile) && Date.now() < deadline) await delay(20);
    if (!exists(pidFile)) throw new Error("native process owner fixture did not start its descendant");
    while (!exists(rootExitFile) && Date.now() < deadline) await delay(20);
    if (!exists(rootExitFile)) throw new Error("native process owner fixture root did not exit");
    const descendantPid = Number(await readFile(pidFile, "utf8"));
    if (!Number.isSafeInteger(descendantPid) || descendantPid <= 0) throw new Error("native process owner fixture wrote an invalid descendant PID");
    let result;
    while (Date.now() < deadline) {
      result = owner.terminate(spawned.token);
      if (result.kind === "terminated") break;
      await delay(20);
    }
    if (result?.kind !== "terminated") throw new Error(`native process owner did not prove cleanup: ${JSON.stringify(result)}`);
    if (processAlive(descendantPid)) throw new Error("native process owner left a root-exits-first descendant alive");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

async function qualifyProcessIo(owner, executable, environment) {
  const echo = "process.stdin.once('data', c => { process.stdout.write(c); process.stderr.write('diagnostic'); process.exit(0); });";
  const spawned = owner.spawn(executable, ["-e", echo], null, environment);
  await owner.writeStdin(spawned.token, Buffer.from("native-io\\n"));
  owner.closeStdin(spawned.token);
  const deadline = Date.now() + 5_000;
  let stdout = "";
  let stderr = "";
  while (Date.now() < deadline) {
    for (const stream of ["stdout", "stderr"]) {
      const value = owner.pollOutput(spawned.token, stream);
      if (value.kind === "error") throw new Error(`native process ${stream} read failed: ${value.reason}`);
      if (value.kind === "data") {
        const text = Buffer.from(value.bytes).toString("utf8");
        if (stream === "stdout") stdout += text;
        else stderr += text;
      }
    }
    if (owner.pollExit(spawned.token).kind === "exited") break;
    await delay(20);
  }
  if (stdout !== "native-io\\n" || stderr !== "diagnostic") {
    throw new Error(`native process stdio mismatch: stdout=${JSON.stringify(stdout)} stderr=${JSON.stringify(stderr)}`);
  }
  if (owner.pollExit(spawned.token).kind !== "exited") throw new Error("native process did not report root exit");
  const result = owner.terminate(spawned.token);
  if (result.kind !== "terminated") throw new Error(`native process stdio cleanup was uncertain: ${JSON.stringify(result)}`);

  const blocked = owner.spawn(
    executable,
    ["-e", "setInterval(() => process.stdout.write('x'.repeat(16384)), 0);"],
    null,
    environment,
  );
  await delay(400);
  let blockedObservation = null;
  const blockedDeadline = Date.now() + 5_000;
  while (Date.now() < blockedDeadline) {
    blockedObservation = owner.pollOutput(blocked.token, "stdout");
    if (blockedObservation.kind === "error") break;
    await delay(20);
  }
  if (blockedObservation?.kind !== "error") {
    throw new Error(`native blocked reader did not retain overflow: ${JSON.stringify(blockedObservation)}`);
  }
  const blockedResult = owner.terminate(blocked.token);
  if (blockedResult.kind !== "terminated") throw new Error(`native blocked reader cleanup was uncertain: ${JSON.stringify(blockedResult)}`);

  const writerBlocked = owner.spawn(
    executable,
    ["-e", "setInterval(() => {}, 100000);"],
    null,
    environment,
  );
  const pendingWrite = owner.writeStdin(writerBlocked.token, Buffer.alloc(64 * 1024));
  let concurrentWriteRejected = false;
  try {
    await owner.writeStdin(writerBlocked.token, Buffer.from("second-write"));
  } catch {
    concurrentWriteRejected = true;
  }
  if (!concurrentWriteRejected) throw new Error("native owner accepted concurrent stdin writes");
  let writeSettled = false;
  void pendingWrite.then(() => { writeSettled = true; }, () => { writeSettled = true; });
  await delay(100);
  const writerResult = owner.terminate(writerBlocked.token);
  if (writerResult.kind !== "terminated") throw new Error(`native blocked writer cleanup was uncertain: ${JSON.stringify(writerResult)}`);
  await delay(100);
  if (!writeSettled) throw new Error("native blocked writer remained pending after owner termination");
}

function exists(path) {
  return existsSync(path);
}

function processAlive(pid) {
  try { process.kill(pid, 0); return true; }
  catch { return false; }
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function qualifyAdapter(bindingPath, engineRoot) {
  const { mock } = await import("bun:test");
  const binding = createRequire(import.meta.url)(bindingPath);
  mock.module(`@acyclic-labs/fs-${process.platform}-${process.arch}`, () => binding);
  const { openNativeFs, openNativeWorkspaceGraph, DEFAULT_OBJECT_CACHE_OPTIONS, portableVolumeOptions } = await import("../typescript/packages/filesystem/dist/native.js");
  const engine = await openNativeFs({
    root: join(engineRoot, "public-adapter"),
    objectCache: {
      ...DEFAULT_OBJECT_CACHE_OPTIONS,
      maximumBytes: Number(DEFAULT_OBJECT_CACHE_OPTIONS.maximumBytes),
    },
  });
  try {
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
        maximumBytes: Number(DEFAULT_OBJECT_CACHE_OPTIONS.maximumBytes),
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
    if (
      Object.keys(read.work).length !== 24
      || Object.values(read.work).some((value) => typeof value !== "number")
      || Object.keys(checkout.acquisitionWork).length !== 24
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
  console.log(`acyclic-fs native TypeScript adapter passed on ${process.platform}-${process.arch}`);
}
