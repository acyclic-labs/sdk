import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { publishReport } from "./report.mjs";

test("complete evidence is published before failed export can release its snapshot", async () => {
  const report = { results: [{ case: "host", status: "fail",
    evidence: { captures: [{ stdout: "real host output", stderr: "native failure", code: 101 }],
      requests: [{ sequence: 1, path: "/actual/request" }] } }] };
  let published;
  const io = { publish: async text => { published = text; },
    writeFile: async () => { throw new Error("ENOTCONN: export snapshot unavailable"); } };
  await assert.rejects(publishReport("matrix.json", report, io), /ENOTCONN/);
  assert.deepEqual(JSON.parse(published), report);
  // Old write-first ordering loses every byte when the export rejects.
  published = undefined;
  await assert.rejects((async () => {
    await io.writeFile("matrix.json", JSON.stringify(report));
    await io.publish(JSON.stringify(report));
  })(), /ENOTCONN/);
  assert.equal(published, undefined);
  const order = [];
  await publishReport("matrix.json", report, {
    publish: async text => order.push(["published", text]),
    writeFile: async (path, text) => order.push([path, text]),
  });
  assert.deepEqual(order.map(([event]) => event), ["published", "matrix.json"]);
  assert.equal(order[0][1], order[1][1]);
});

test("cancellation during either original report write retains a failed terminal snapshot", async () => {
  const root = await mkdtemp(join(tmpdir(), "report-cancellation-"));
  try {
    for (const boundary of ["before", "publish", "file", "none"]) {
      const signal = new AbortController(), report = { pass: true, results: [] };
      const snapshots = [], path = join(root, boundary + ".json");
      const reason = new Error("caller cancelled at " + boundary);
      if (boundary === "before") signal.abort(reason);
      const io = {
        publish: async text => {
          await writeFile(join(root, boundary + ".stdout"), text, { flag: "a" });
          snapshots.push(JSON.parse(text));
          if (boundary === "publish") signal.abort(reason);
        },
        writeFile: async (path, text) => {
          await writeFile(path, text);
          if (boundary === "file") signal.abort(reason);
        },
      };
      await publishReport(path, report, io, signal.signal);
      const expected = boundary === "none";
      assert.equal(report.pass, expected);
      assert.deepEqual(JSON.parse(await readFile(path, "utf8")), report);
      assert.deepEqual(snapshots.at(-1), report);
      assert.equal(snapshots.length, ["publish", "file"].includes(boundary) ? 2 : 1);
      if (!expected) assert.equal(report.cancellation, reason.message);
      if (snapshots.length === 2) assert.equal(snapshots[0].pass, true);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
