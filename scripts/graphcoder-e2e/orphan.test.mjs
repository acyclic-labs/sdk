import assert from "node:assert/strict";
import test from "node:test";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Spool, Processes } from "./process.mjs";
import { fenced } from "./orphan-check.mjs";
import { expand, seed } from "./fixtures.mjs";

test("orphan endpoint input works without ambient inheritance", async () => {
  const root = await mkdtemp(join(tmpdir(), "orphan-input-"));
  const processes = new Processes({ transcripts: root });
  const server = createServer();
  const responses = [];
  server.on("request", (_, response) => responses.push(response));
  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const url = `http://127.0.0.1:${server.address().port}/orphan-fencing/v1`;
    const scenario = JSON.parse(await readFile(new URL("./cases/orphan-fencing.json", import.meta.url), "utf8"));
    await seed(root, expand(scenario.workspace.files, { url }));
    const helper = fileURLToPath(new URL("./orphan.mjs", import.meta.url));

    const missingInput = new AbortController();
    try {
      const unexpected = once(server, "request", { signal: missingInput.signal });
      const missing = processes.start(process.execPath, [helper, "tree"], { cwd: root, env: { E2E_MOCK_URL: url } });
      const refused = await Promise.race([missing.closed.then(result => ({ result })),
        unexpected.then(() => ({}))]);
      assert.ok(refused.result, "ambient endpoint admitted without explicit input");
      assert.equal(refused.result.code, 1);
      assert.match(refused.result.stderr, /step=admit-input failed:.*explicit endpoint file.*recovery command:/);
    } finally { missingInput.abort(); }
    assert.equal(responses.length, 0);

    const request = once(server, "request");
    const tree = processes.start(process.execPath, [helper, "tree", "orphan-endpoint.txt"], { cwd: root, env: {} });
    const admitted = await Promise.race([request.then(value => ({ value })),
      tree.closed.then(result => ({ result }))]);
    assert.ok(admitted.value, `explicit endpoint failed before barrier: ${admitted.result?.stderr}`);
    const [incoming, response] = admitted.value;
    assert.equal(incoming.url, "/orphan-fencing/v1/orphan-ready");
    assert.equal(incoming.method, "POST");
    const body = [];
    for await (const chunk of incoming) body.push(chunk);
    const record = { path: "orphan-ready", body: JSON.parse(Buffer.concat(body).toString()) };
    assert.equal(record.body.parent_pid, tree.child.pid);
    assert.throws(() => fenced([record], "orphan-ready"), /survived owner recovery/);
    response.end();
    const released = await tree.closed;
    assert.equal(released.code, 1);
    assert.match(released.stderr, /step=hold-barrier failed: orphan probe was released instead of fenced/);
    fenced([record], "orphan-ready");
    assert.equal(responses.length, 1);
  } finally {
    for (const response of responses) response.end();
    server.closeAllConnections();
    await processes.close();
    await new Promise(resolve => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  }
});

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
    await processes.close();
    await Promise.all(children.map(child => child.closed));
    fenced(records, "probe");
    assert.throws(() => fenced([], "probe"), /committed exactly once/);
    assert.throws(() => fenced([...records, ...records], "probe"), /committed exactly once/);
  } finally { await processes.close(); }
});
