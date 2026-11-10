import assert from "node:assert/strict";
import { once } from "node:events";
import { readFile, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { join } from "node:path";
import { conversation, waitFor } from "./conversation.mjs";

export async function shellJobs(processes, input, fixture, capture) {
  const sockets = new Set(), ready = new Map();
  const pending = new Map(["one", "two"].map(name => [name, new Promise(resolve => ready.set(name, resolve))]));
  const server = createServer(socket => {
    sockets.add(socket);
    socket.on("error", () => {}); // Cancelling the real child may reset TCP.
    let name = "";
    socket.on("data", bytes => {
      name += bytes;
      if (name.endsWith("\n") && ready.has(name.trim())) {
        ready.get(name.trim())(socket);
        ready.delete(name.trim());
      }
    });
  });
  let jobs, inspected;
  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening", { signal: fixture.signal });
    await writeFile(join(fixture.workspace, "shell-gate.json"), JSON.stringify({ port: server.address().port }));
    await conversation(processes, fixture.binary, fixture, input, capture, async (turn, active, signal) => {
      if (turn.id !== "inspect") return;
      const one = await waitFor(pending.get("one"), signal);
      const two = await waitFor(pending.get("two"), signal);
      const closed = new Promise(resolve => one.once("close", resolve));
      await writeFile(join(fixture.workspace, jobs.one.cancel), "cancel\n");
      await waitFor(closed, signal);
      assert.equal(two.destroyed, false, "cancelling one job stopped its sibling");
      let text = "", receive;
      const completed = new Promise(resolve => {
        receive = bytes => { text += bytes; if (text.includes("\n")) resolve(text); };
        two.on("data", receive);
      });
      try {
        two.write("G");
        assert.equal(await waitFor(completed, signal), "OUTPUT_COMPLETE\n");
      } finally { two.off("data", receive); }
    }, async (turn, reply, signal) => {
      if (turn.id === "inspect") { inspected = JSON.parse(reply.text); return; }
      if (turn.id === "foreground") {
        const later = JSON.parse(reply.text);
        assert.deepEqual(later.one, jobs.one);
        assert.deepEqual(later.two, jobs.two);
        assert.ok(later.foreground.includes("FOREGROUND_PROGRESS"));
        for (const name of ["one", "two"])
          assert.equal((await waitFor(pending.get(name), signal)).destroyed, false);
        return;
      }
      if (turn.id !== "start") return;
      jobs = JSON.parse(reply.text);
      for (const name of ["one", "two"]) {
        assert.equal(jobs[name].background, true);
        assert.equal(typeof jobs[name].cancel, "string");
        await waitFor(pending.get(name), signal);
      }
    });
    // The final model turn waits on private-file publication and verifies the
    // original terminal before closing the conversation's owner.
    assert.equal(inspected.one.stop.kind, "control_stopped");
    assert.equal(inspected.one.stop.cleanup_completed, true);
    const succeeded = value => {
      assert.equal(value.success, true, "background job did not finish successfully");
      assert.equal(value.exit_code, 0, "background job exit was not zero");
    };
    succeeded(inspected.two);
    assert.throws(() => succeeded({ ...inspected.two, success: false, exit_code: 7 }),
      /background job did not finish successfully/);
    assert.ok(inspected.two.omitted_bytes > 0);
    assert.ok(inspected.verified.includes("COMPLETE_SPILL_AND_NEGATIVE_CONTROL"));
    await assert.rejects(readFile(join(fixture.workspace, "one.completed")), { code: "ENOENT" });
    assert.equal(await readFile(join(fixture.workspace, "two.completed"), "utf8"), "committed\n");
    capture({ shell_jobs: inspected });
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
}
