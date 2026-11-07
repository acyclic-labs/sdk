// Supply bytes recorded by an earlier real invocation through a native pipe.
// The original producer releases a blocking reader; its loss ends the check.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Spool } from "./process.mjs";

const script = fileURLToPath(import.meta.url);

export async function withFileSource(processes, fixture, source, invoke) {
  if (!source) return invoke(undefined, running => running.closed);
  const path = process.platform === "win32"
    ? `\\\\.\\pipe\\graphcoder-${randomUUID()}` : join(fixture.workspace, `input-${randomUUID()}.pipe`);
  if (process.platform !== "win32") {
    const made = await processes.run("mkfifo", [path], { env: fixture.env });
    assert.equal(made.code, 0, `step=create input pipe; recovery=mkfifo '${path}'; ${made.stderr}`);
  }
  const producer = processes.start(process.execPath, [script, path, source], { env: fixture.env });
  producer.input.end();
  const events = new Spool(producer.child.stdout, producer.closed, { key: event => [event.event], unique: true });
  try {
    await events.keyed("ready");
    return await invoke(path, async running => {
      try {
        return await Promise.race([running.closed, producer.closed.then(result => {
          assert.equal(result.code, 0, `step=supply recorded input; recovery=node '${script}' '${path}' '${source}'; ${result.stderr}`);
          return running.closed;
        })]);
      } catch (error) {
        running.killExecutable();
        await running.closed;
        throw error;
      }
    });
  } finally {
    producer.killExecutable();
    await producer.closed;
  }
}

async function supply(path, source) {
  const bytes = await readFile(source);
  if (process.platform === "win32") {
    const server = createServer(socket => socket.end(bytes, () => server.close()));
    const closed = new Promise((done, failed) => {
      server.once("close", done);
      server.once("error", failed);
    });
    closed.catch(() => {});
    await new Promise((ready, failed) => {
      server.once("error", failed);
      server.listen(path, ready);
    });
    console.log(JSON.stringify({ event: "ready" }));
    await closed;
  } else {
    console.log(JSON.stringify({ event: "ready" }));
    try { await writeFile(path, bytes); }
    catch (error) { if (error.code !== "EPIPE") throw error; }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === script) {
  const [path, source] = process.argv.slice(2);
  try { await supply(path, source); }
  catch (error) {
    console.error(`step=supply recorded input; recovery=node '${script}' '${path}' '${source}'; ${error.message}`);
    process.exitCode = 1;
  }
}
