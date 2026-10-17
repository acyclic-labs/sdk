import assert from "node:assert/strict";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";

const installed = process.env.FS_INSTALLED_PACKAGE_ROOT ?? process.argv[2];
assert.ok(installed, "pass the installed package root or the native .node binding");
const native = installed.endsWith(".node") ? {
  openNativeFs: ({ root, objectCache }) => createRequire(import.meta.url)(resolve(installed))
    .NativeFs.open(root, objectCache),
  DEFAULT_OBJECT_CACHE_OPTIONS: {
    maximumEntries: 8, maximumBytes: 1_048_576n,
    maximumInFlight: 2, maximumWaitersPerObject: 2,
  },
} : await import(pathToFileURL(join(resolve(installed), "dist/native.js")).href);
const child = promisify(execFile);
const read = (path) => child(process.execPath, ["-e",
  "process.stdout.write(require('node:fs').readFileSync(process.argv[1]))", path,
], { windowsHide: true }).then(({ stdout }) => stdout);
const write = (path, value) => child(process.execPath, ["-e",
  "require('node:fs').writeFileSync(process.argv[1],process.argv[2])", path, value,
], { windowsHide: true });
const root = await mkdtemp(join(tmpdir(), "acyclic-authorized-napi-mount-"));
const engine = await native.openNativeFs({
  root: join(root, "engine"), objectCache: { ...native.DEFAULT_OBJECT_CACHE_OPTIONS },
});
try {
  const workspace = await engine.createWorkspace("authenticated-mount");
  await workspace.write("/content", Buffer.from("original"));
  for (const writable of [false, true]) {
    const destination = await mkdtemp(join(root, writable ? "write-" : "read-"));
    const options = {
      writable, subdirectory: "/", publication: "close-and-sync",
    };
    if (process.platform === "win32" && !writable) {
      await assert.rejects(workspace.mount(destination, options),
        /ProjFS cannot prevent host mutations on read-only mounts/);
      assert.deepEqual(await readdir(destination), []);
      assert.equal(await workspace.read("/content", 64n).then((bytes) =>
        new TextDecoder().decode(bytes)), "original");
      continue;
    }
    const mount = await workspace.mount(destination, options)
      .catch((cause) => { throw new Error(`mount writable=${writable}`, { cause }); });
    let failure;
    try {
      assert.equal(await read(join(destination, "content")), "original");
      if (writable) {
        await write(join(destination, "content"), "updated");
        await mount.sync();
        assert.equal(new TextDecoder().decode(await workspace.read("/content", 64n)), "updated");
      } else {
        await assert.rejects(write(join(destination, "content"), "forbidden"));
      }
    } catch (error) {
      failure = error;
      throw error;
    } finally {
      try {
        assert.equal(await mount.unmount(), true);
      } catch (cleanup) {
        throw failure ? new AggregateError([failure, cleanup], "mount operation and cleanup failed")
          : new Error(`unmount writable=${writable}`, { cause: cleanup });
      }
    }
  }
} finally {
  engine.cancel();
  await rm(root, { recursive: true, force: true });
}
