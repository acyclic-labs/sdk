import { startConformanceFixture } from "file:///tmp/c8-fixture-sdk/typescript/packages/actors/test/grpc-conformance-fixture.mjs";
import { createServer } from "node:http";
import { writeFile } from "node:fs/promises";

const state = { started: 0, aborted: 0, active: 0 };
const control = createServer((req, res) => {
  if (req.url === "/state") { res.writeHead(200, {"content-type":"application/json"}); res.end(JSON.stringify(state)); return; }
  res.writeHead(404); res.end();
});
const pending = new Map();
const fixture = await startConformanceFixture({
  onUnaryRequest: async (method, request, context) => {
    if (method.name !== "InspectActor" || !request.actorId.startsWith("pending-c8-799-")) return;
    state.started++; state.active++;
    await new Promise(resolve => {
      let finished = false;
      const finish = () => {
        if (finished) return;
        finished = true;
        context.signal?.removeEventListener("abort", finish);
        pending.delete(request.actorId);
        state.active--; state.aborted++;
        resolve();
      };
      pending.set(request.actorId, finish);
      if (context.signal?.aborted) finish();
      else context.signal?.addEventListener("abort", finish, { once: true });
    });
  },
});
await new Promise(resolve => control.listen(0, "localhost", resolve));
const options = { ...fixture.options, controlEndpoint: `http://localhost:${control.address().port}` };
const output = process.argv[2];
await writeFile(output, JSON.stringify(options));
const close = async () => { try { await new Promise(resolve => control.close(resolve)); await fixture.close(); } finally { process.exit(0); } };
process.on("SIGINT", close); process.on("SIGTERM", close); setInterval(() => {}, 1000);
