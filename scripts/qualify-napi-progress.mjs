import assert from "node:assert/strict";
import { createServer } from "node:net";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { Worker, isMainThread, workerData } from "node:worker_threads";

// Compile the fixture as a cdylib, then copy its platform library to a .node
// filename. The caller supplies that exact addon; no installed SDK is replaced.
if (!isMainThread) {
  const addon = createRequire(import.meta.url)(workerData.addon);
  addon[workerData.method](() => {
    throw new Error("queued callback must not run during shutdown");
  }, workerData.port, workerData.abortQueue);
  // Exit on this same JS stack. Ordinary shutdown can attempt delivery into
  // a terminating VM; the explicit abort case requests queued-item disposal.
  process.exit(0);
} else {
  const addon = process.argv[2];
  if (!addon) {
    throw new Error("step=addon-input; recovery: node scripts/qualify-napi-progress.mjs /absolute/path/to/fixture.node");
  }
  for (const [method, abortQueue] of [
    ["queueDirect", false], ["queueDirect", true],
    ["queueErrorFirst", false], ["queueErrorFirst", true],
  ]) {
    const server = createServer();
    await new Promise((done, fail) => {
      server.once("error", fail);
      server.listen(0, "127.0.0.1", done);
    });
    let worker;
    let socket;
    let escape;
    try {
      await new Promise((done, fail) => {
        // This escape is an assertion failure, never a retry or a progress
        // mechanism. The actual completion is the native task's socket event.
        escape = setTimeout(() => fail(new Error(
          `step=callback-progress method=${method} abort=${abortQueue}; recovery: repair queued callback disposal, rebuild the fixture, then rerun node scripts/qualify-napi-progress.mjs ${resolve(addon)}`,
        )), 10_000);
        server.once("connection", (connected) => {
          socket = connected;
          let text = "";
          connected.setEncoding("utf8");
          connected.on("error", fail);
          connected.on("data", (bytes) => {
            text += bytes;
          });
          connected.once("end", () => {
            try {
              assert.equal(text, `queued\n${abortQueue ? "producer-lost" : "environment-stopped"}\n`);
              done();
            } catch (error) { fail(error); }
          });
        });
        worker = new Worker(new URL(import.meta.url), {
          workerData: { addon: resolve(addon), method, abortQueue, port: server.address().port },
        });
        worker.once("error", fail);
        worker.once("exit", (code) => {
          if (code !== 0) fail(new Error(`unexpected worker exit ${code}`));
        });
      });
      console.log(`PASS ${method} abort=${abortQueue}: typed completion after Worker termination`);
    } finally {
      clearTimeout(escape);
      await worker?.terminate();
      socket?.destroy();
      await new Promise((done) => server.close(done));
    }
  }
}
