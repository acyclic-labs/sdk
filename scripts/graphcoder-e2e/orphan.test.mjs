import assert from "node:assert/strict";
import test from "node:test";
import { once } from "node:events";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import { Spool, Processes } from "./process.mjs";
import { alive, fenced } from "./orphan-check.mjs";

test("orphan fixture requires its admitted endpoint and carries it through the real tree", async () => {
  const fixture = fileURLToPath(new URL("./orphan.mjs", import.meta.url));
  for (const mode of ["missing endpoint", "absent environment", "different environment"]) {
    const processes = new Processes();
    let receive;
    const request = new Promise(resolve => { receive = resolve; });
    const server = createServer((req, _res) => {
      let body = "";
      req.setEncoding("utf8");
      req.on("data", chunk => { body += chunk; });
      req.on("end", () => receive({ path: req.url, body: JSON.parse(body) }));
    });
    try {
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const endpoint = `http://127.0.0.1:${server.address().port}/selected/v1`;
      const env = { ...process.env };
      delete env.E2E_MOCK_URL;
      if (mode === "missing endpoint") env.E2E_MOCK_URL = endpoint;
      if (mode === "different environment") env.E2E_MOCK_URL = `${endpoint}/wrong`;
      const args = [fixture, "tree", ...(mode === "missing endpoint" ? [] : [endpoint])];
      const tree = processes.startOwned(process.execPath, args, { env });
      const first = await Promise.race([
        request.then(probe => ({ probe })),
        tree.closed.then(result => ({ result })),
      ]);
      if (mode === "missing endpoint") {
        assert.ok(first.result, "an inherited endpoint cannot admit the orphan probe");
        assert.notEqual(first.result.code, 0);
        assert.match(first.result.stderr, /explicit endpoint required/);
      } else {
        assert.ok(first.probe, first.result?.stderr);
        assert.equal(first.probe.path, "/selected/v1/orphan-ready");
        const { pid, parent_pid } = first.probe.body;
        assert.notEqual(pid, parent_pid);
        assert.ok(alive(pid) && alive(parent_pid), "both generations must remain held at the request");
        await processes.close();
        fenced([{ path: "probe", body: first.probe.body }], "probe");
      }
    } finally {
      await processes.close();
      server.closeAllConnections();
      await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    }
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
    await Promise.all(children.map(child => child.closed));
    fenced(records, "probe");
    assert.throws(() => fenced([], "probe"), /committed exactly once/);
    assert.throws(() => fenced([...records, ...records], "probe"), /committed exactly once/);
  } finally { await processes.close(); }
});
