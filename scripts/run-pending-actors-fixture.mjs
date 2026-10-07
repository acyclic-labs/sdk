import { createServer } from "node:http";
import { writeFile } from "node:fs/promises";
import { startConformanceFixture } from "../typescript/packages/actors/test/grpc-conformance-fixture.mjs";

// Qualification clients use the control endpoint to prove cancellation while
// an authenticated Rust operation is still waiting on the server.
const [optionsPath, certificatePath] = process.argv.slice(2);
if (!optionsPath || !certificatePath || process.argv.length !== 4) {
  throw new Error("usage: node scripts/run-pending-actors-fixture.mjs OPTIONS_JSON CA_PEM");
}
const state = { started: 0, aborted: 0, active: 0 };
const releases = new Set();
const fixture = await startConformanceFixture({
  async onUnaryRequest(method, request, context) {
    if (method.name !== "InspectActor" || !request.actorId.startsWith("pending-")) return;
    state.started++;
    state.active++;
    await new Promise((resolve, reject) => {
      let finished = false;
      const finish = (aborted) => {
        if (finished) return;
        finished = true;
        releases.delete(release);
        context.signal.removeEventListener("abort", abort);
        state.active--;
        if (aborted) {
          state.aborted++;
          reject(context.signal.reason ?? new Error("client cancelled pending operation"));
        } else resolve();
      };
      const release = () => finish(false);
      const abort = () => finish(true);
      releases.add(release);
      context.signal.addEventListener("abort", abort, { once: true });
      if (context.signal.aborted) abort();
    });
  },
});
const control = createServer((request, response) => {
  if (request.method === "POST" && request.url === "/release") {
    for (const release of [...releases]) release();
  } else if (request.method !== "GET" || request.url !== "/state") {
    response.writeHead(404).end();
    return;
  }
  response.writeHead(200, { "content-type": "application/json" });
  response.end(JSON.stringify(state));
});
await new Promise(resolve => control.listen(0, "127.0.0.1", resolve));
const options = {
  ...fixture.options,
  controlEndpoint: `http://127.0.0.1:${control.address().port}`,
};
await writeFile(optionsPath, JSON.stringify(options));
await writeFile(certificatePath, options.caCertificate);
process.stdout.write(`Pending Actors fixture ready; options saved to ${optionsPath}\n`);
const stop = async () => {
  for (const release of [...releases]) release();
  await new Promise(resolve => control.close(resolve));
  await fixture.close();
  process.exit(0);
};
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
