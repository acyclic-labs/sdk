import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { exerciseWorkspace } from "./workspace-composition.mjs";

const [packageRoot, companionName, expectedDigest] = process.argv.slice(2);
if (packageRoot === undefined || companionName === undefined || !/^sha256:[0-9a-f]{64}$/u.test(expectedDigest ?? "")) {
  throw new Error("usage: native-public-installed.mjs INSTALLED_PACKAGE_ROOT COMPANION_NAME sha256:EXPECTED_NATIVE_DIGEST");
}

const packagePath = resolve(packageRoot);
const installedRequire = createRequire(pathToFileURL(join(packagePath, "generated", "native", "binding.cjs")));
const installedPath = installedRequire.resolve(companionName);
const digest = createHash("sha256");
for await (const chunk of createReadStream(installedPath)) digest.update(chunk);
if (`sha256:${digest.digest("hex")}` !== expectedDigest) {
  throw new Error("installed public consumer native artifact digest differs from the qualified producer");
}
const qualifiedBinding = installedRequire(installedPath);
if (installedRequire("./binding.cjs") !== qualifiedBinding) {
  throw new Error("installed public consumer loader did not select the qualified companion");
}
const native = await import(pathToFileURL(join(packagePath, "dist/native.js")).href);
const engineRoot = process.env.FS_NATIVE_TEST_ROOT === undefined
  ? await mkdtemp(join(tmpdir(), "acyclic-fs-public-loader-"))
  : resolve(process.env.FS_NATIVE_TEST_ROOT);
const removeEngineRoot = process.env.FS_NATIVE_TEST_ROOT === undefined;

const engine = await native.openNativeFs({
  root: engineRoot,
  objectCache: {
    ...native.DEFAULT_OBJECT_CACHE_OPTIONS,
  },
});

try {
  await exerciseWorkspace(engine);
  const workspace = await engine.createWorkspace("public-loader");
  const before = await workspace.sync();
  const committed = await workspace.write("/public-loader.txt", new TextEncoder().encode("native-loader"));
  if (committed.status !== "committed") {
    throw new Error(`public openNativeFs write returned ${committed.status}`);
  }
  const content = await workspace.read("/public-loader.txt", 64n);
  if (new TextDecoder().decode(content) !== "native-loader") {
    throw new Error("public openNativeFs read did not preserve bytes");
  }
  const after = await workspace.sync();
  const changes = await workspace.diff(before, after, 32);
  if (changes.changes().files.length === 0) {
    throw new Error("public openNativeFs change set omitted the authored file");
  }
  console.log(`public openNativeFs installed loader passed on ${process.platform}-${process.arch}`);
} finally {
  engine.cancel();
  if (removeEngineRoot) await rm(engineRoot, { recursive: true, force: true });
}
