import assert from "node:assert/strict";
import test from "node:test";
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
