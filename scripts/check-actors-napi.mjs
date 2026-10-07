import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

// This qualification script never builds or selects a transport. It loads the
// Rust-owned N-API bridge produced by acyclic-actors-napi and checks its stable
// generated surface before an artifact is staged for a platform package.
const libraryNames = {
  win32: "acyclic_actors_napi.dll",
  linux: "libacyclic_actors_napi.so",
  darwin: "libacyclic_actors_napi.dylib",
};
const libraryName = libraryNames[process.platform];
if (libraryName === undefined) {
  throw new Error(`unsupported Actors N-API qualification platform ${process.platform}`);
}

const { version } = JSON.parse(await readFile(
  new URL("../typescript/packages/actors/package.json", import.meta.url), "utf8",
));
if (typeof version !== "string" || !/^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
  throw new Error("invalid Actors package version");
}

const childBinding = process.env.ACYCLIC_ACTORS_NAPI_CHILD_BINDING;
if (childBinding !== undefined) {
  await qualify(childBinding);
  process.exit(0);
}

const positional = process.argv.slice(2);
const output = positional[0];
if (positional.length > 1 || (output !== undefined && !isAbsolute(output))) {
  throw new Error("usage: check-actors-napi.mjs [ABSOLUTE_OUTPUT]");
}

const targetRoot = resolve(process.env.CARGO_TARGET_DIR ?? "target");
const profile = process.env.ACYCLIC_ACTORS_NAPI_PROFILE ?? "debug";
const source = join(targetRoot, profile, libraryName);
const temporary = await mkdtemp(join(tmpdir(), "acyclic-actors-napi-"));
const bindingPath = join(temporary, `${basename(libraryName)}.node`);
try {
  await copyFile(source, bindingPath);
  await new Promise((resolveChild, rejectChild) => {
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url)], {
      env: {
        ...process.env,
        ACYCLIC_ACTORS_NAPI_CHILD_BINDING: bindingPath,
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
    // Stage only the private copy loaded by the successful child.
    await mkdir(dirname(output), { recursive: true });
    await mkdir(output);
    const name = `acyclic-actors-${version}-${process.platform}-${process.arch}.node`;
    await copyFile(bindingPath, join(output, name));
    const digest = createHash("sha256");
    for await (const chunk of createReadStream(join(output, name))) digest.update(chunk);
    await writeFile(join(output, "SHA256SUMS"), `${digest.digest("hex")}  ${name}\n`, { flag: "wx" });
  }
} finally {
  await rm(temporary, { recursive: true, force: true });
}

async function qualify(bindingPath) {
  const binding = createRequire(import.meta.url)(bindingPath);
  if (typeof binding.NativeActorsClient !== "function") {
    throw new Error("Rust N-API bridge did not export NativeActorsClient");
  }
  if (typeof binding.NativeActorsCancellation !== "function") {
    throw new Error("Rust N-API bridge did not export NativeActorsCancellation");
  }
  if (binding.NativeActorsClient.version() !== version) {
    throw new Error("Actors N-API version does not match the package");
  }
  const cancellation = new binding.NativeActorsCancellation();
  if (cancellation.cancelled) {
    throw new Error("Actors N-API cancellation handle started cancelled");
  }
  cancellation.cancel();
  if (!cancellation.cancelled) {
    throw new Error("Actors N-API cancellation handle did not become cancelled");
  }
  cancellation.cancel();
  if (!cancellation.cancelled) {
    throw new Error("Actors N-API cancellation handle was not monotonic");
  }
  console.log(`acyclic-actors N-API ABI passed on ${process.platform}-${process.arch}`);
}
