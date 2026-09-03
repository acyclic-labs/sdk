import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { spawn } from "node:child_process";

import { openHostedFs } from "../dist/hosted.js";

const root = await mkdtemp(join(tmpdir(), "acyclic-fs-hosted-"));
const executable = resolve(
  "../../..",
  process.platform === "win32" ? "target/debug/fsd.exe" : "target/debug/fsd",
);
const child = spawn(executable, ["--root", root], { stdio: ["ignore", "pipe", "inherit"] });

try {
  const lines = createInterface({ input: child.stdout });
  const startupEvent = await Promise.race([
    once(lines, "line"),
    once(child, "error").then(([error]) => { throw error; }),
    once(child, "exit").then(([code]) => { throw new Error(`daemon exited ${code} before startup`); }),
  ]);
  const startup = JSON.parse(startupEvent[0]);
  const fs = await openHostedFs({
    endpoint: startup.httpEndpoint,
    bearerToken: startup.httpBearerToken,
  });
  const workspace = await fs.createWorkspace("hosted-e2e");
  const initial = await workspace.sync();
  const first = await workspace.write("/hello.txt", new TextEncoder().encode("hello"));
  if (first.status !== "committed") throw new Error(`unexpected write status ${first.status}`);
  const transaction = await workspace.beginTransaction(
    Uint8Array.from({ length: 16 }, (_, index) => index + 1),
  );
  await transaction.writeRange("/hello.txt", 5n, new TextEncoder().encode(" hosted"));
  await transaction.createDirAll("/nested/path");
  await transaction.write("/nested/path/file", new Uint8Array([1, 2, 3]));
  const committed = await transaction.commit();
  const retried = await transaction.commit();
  if (committed.status !== "committed" || retried.status !== "already-committed") {
    throw new Error(`transaction retry was not idempotent: ${committed.status}/${retried.status}`);
  }
  if (Buffer.compare(Buffer.from(committed.generationId), Buffer.from(retried.generationId)) !== 0) {
    throw new Error("transaction retry changed generation identity");
  }
  await transaction.close();
  const bytes = await workspace.read("/hello.txt", 64n);
  if (new TextDecoder().decode(bytes) !== "hello hosted") throw new Error("hosted read diverged");
  const current = await workspace.sync();
  const changes = await workspace.diff(initial, current, 64);
  if (changes.changes().files.length === 0 || changes.changes().bindings.length === 0) {
    throw new Error("hosted semantic diff is incomplete");
  }
  const fork = await workspace.forkAt("historical", initial);
  let absent = false;
  try { await fork.read("/hello.txt", 64n); } catch (error) { absent = error?.code === "engine_failure"; }
  if (!absent) throw new Error("exact-generation fork did not preserve historical state");
  fs.close();

  const shutdown = await fetch(startup.httpEndpoint, {
    method: "POST",
    headers: { authorization: `Bearer ${startup.httpBearerToken}`, "content-type": "application/json" },
    body: JSON.stringify({ id: "shutdown", method: "shutdown" }),
  });
  if (!shutdown.ok) throw new Error(`daemon shutdown failed with ${shutdown.status}`);
  const [code] = await once(child, "exit");
  if (code !== 0) throw new Error(`daemon exited ${code}`);
} finally {
  if (child.exitCode === null) child.kill();
  await rm(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
}
