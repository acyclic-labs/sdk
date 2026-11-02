import assert from "node:assert/strict";
import { once } from "node:events";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import test from "node:test";
import { Processes } from "./process.mjs";

test("native process completion joins the original transcript descriptor closes", async () => {
  const originalOpen = fs.openSync, originalClose = fs.close;
  const descriptors = new Set(), deferred = [];
  const requested = Promise.withResolvers();
  const scope = new Processes();
  let completed = false, scopeCompleted = false;
  try {
    fs.openSync = (...args) => {
      const fd = originalOpen(...args);
      if (/\.(stdout|stderr)$/.test(String(args[0]))) descriptors.add(fd);
      return fd;
    };
    fs.close = (fd, callback) => {
      if (!descriptors.has(fd)) return originalClose(fd, callback);
      deferred.push(() => originalClose(fd, callback));
      if (deferred.length === 2) requested.resolve();
    };
    syncBuiltinESMExports();
    const child = scope.start(process.execPath, ["-e",
      "process.stdout.write('original λ output'); process.stderr.write('original 終 error');"]);
    const closed = child.closed.then(result => { completed = true; return result; });
    const reaped = once(child.child, "close");
    child.input.end();
    // Both events have producers on this exact path: the native child closes
    // its pipes and each real WriteStream asks to close its admitted handle.
    await Promise.all([reaped, requested.promise]);
    assert.equal(completed, false, "finish cannot certify physical descriptor retirement");
    const closing = scope.close().then(() => { scopeCompleted = true; });
    await Promise.resolve();
    assert.equal(scopeCompleted, false, "a reaped child still owns its pending transcript handles");
    for (const fd of descriptors) assert.ok(fs.fstatSync(fd).isFile());
    for (const release of deferred.splice(0)) release();
    const result = await closed;
    await closing;
    assert.equal(result.code, 0);
    assert.equal(result.stdout, "original λ output");
    assert.equal(result.stderr, "original 終 error");
    for (const fd of descriptors) assert.throws(() => fs.fstatSync(fd), { code: "EBADF" });
  } finally {
    for (const release of deferred.splice(0)) release();
    fs.openSync = originalOpen; fs.close = originalClose;
    syncBuiltinESMExports();
    await scope.close();
    fs.rmSync(scope.transcripts, { recursive: true, force: true });
  }
});

test("native transcript handle withdrawal preserves the real OS error and joins its sibling", async () => {
  const originalOpen = fs.openSync, originalCreate = fs.createWriteStream;
  const descriptors = new Set();
  let stdout, originalError;
  const scope = new Processes();
  try {
    fs.openSync = (...args) => {
      const fd = originalOpen(...args);
      if (/\.(stdout|stderr)$/.test(String(args[0]))) descriptors.add(fd);
      if (/\.stdout$/.test(String(args[0]))) stdout = fd;
      return fd;
    };
    fs.createWriteStream = (...args) => {
      const file = originalCreate(...args);
      if (args[1]?.fd === stdout) {
        file.once("error", error => { originalError ??= error; });
        // Withdraw the SAME admitted OS handle before its first actual write.
        // The real filesystem supplies EBADF; no synthetic provider error.
        fs.closeSync(stdout);
      }
      return file;
    };
    syncBuiltinESMExports();
    const child = scope.start(process.execPath, ["-e",
      "process.stdout.write(Buffer.alloc(1048576,97)); process.stderr.write('sibling');"]);
    child.input.end();
    await assert.rejects(child.closed, error => {
      assert.equal(originalError?.code, "EBADF");
      assert.equal(error.kind, "spool");
      assert.equal(error.cause, originalError);
      return true;
    });
    await scope.close();
    for (const fd of descriptors) assert.throws(() => fs.fstatSync(fd), { code: "EBADF" });
    assert.equal(scope.children.size, 0);
  } finally {
    fs.openSync = originalOpen; fs.createWriteStream = originalCreate;
    syncBuiltinESMExports();
    await scope.close();
    fs.rmSync(scope.transcripts, { recursive: true, force: true });
  }
});

test("native transcript withdrawal without an error settles at its original close", async () => {
  const originalCreate = fs.createWriteStream;
  const scope = new Processes();
  let withdrawn;
  try {
    fs.createWriteStream = (...args) => {
      const file = originalCreate(...args);
      if (!withdrawn) {
        withdrawn = file;
        file.once("pipe", () => file.destroy());
      }
      return file;
    };
    syncBuiltinESMExports();
    const child = scope.start(process.execPath, ["-e", "process.stdout.write(Buffer.alloc(1048576,97));"]);
    child.input.end();
    await assert.rejects(child.closed, error => error.kind === "spool"
      && error.cause.code === "ERR_STREAM_PREMATURE_CLOSE");
    assert.equal(withdrawn.closed, true);
    await scope.close();
    assert.equal(scope.children.size, 0);
  } finally {
    fs.createWriteStream = originalCreate;
    syncBuiltinESMExports();
    await scope.close();
    fs.rmSync(scope.transcripts, { recursive: true, force: true });
  }
});
