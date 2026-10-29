import assert from "node:assert/strict";
import test from "node:test";
import { Spool, Processes } from "./process.mjs";
import { fenced } from "./orphan-check.mjs";

test("orphan oracle rejects real live processes and accepts their confirmed exits", async () => {
  const processes = new Processes();
  try {
    const children = Array.from({ length: 2 }, () => processes.start(process.execPath,
      ["-e", "console.log(JSON.stringify({pid:process.pid}));process.stdin.resume()"]));
    const identities = await Promise.all(children.map(async child =>
      (await new Spool(child.child.stdout, child.closed).next(event => event.pid)).pid));
    const records = [{ path: "probe", body: { pid: identities[0], parent_pid: identities[1] } }];
    // Negative control: the barrier alone must never qualify a live process.
    assert.throws(() => fenced(records, "probe"), /survived owner recovery/);
    await Promise.all(children.map(child => child.closed));
    fenced(records, "probe");
    assert.throws(() => fenced([], "probe"), /committed exactly once/);
    assert.throws(() => fenced([...records, ...records], "probe"), /committed exactly once/);
  } finally { await processes.close(); }
});
