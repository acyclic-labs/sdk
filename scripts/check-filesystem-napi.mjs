import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
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
  process.exit(0);
}

const output = process.argv[2];
if (process.argv.length > 3 || (output !== undefined && !isAbsolute(output))) {
  throw new Error("usage: check-filesystem-napi.mjs [ABSOLUTE_OUTPUT]");
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
  const committed = await workspace.write("/abi.txt", Buffer.from("napi"));
  if (committed.status !== "committed") {
    throw new Error(`N-API write returned ${committed.status}`);
  }
  const bytes = await workspace.read("/abi.txt", 16n);
  if (!Buffer.from(bytes).equals(Buffer.from("napi"))) {
    throw new Error("N-API read did not preserve bytes");
  }
  fs.close();
  console.log(`acyclic-fs N-API ABI passed on ${process.platform}-${process.arch}`);
}
