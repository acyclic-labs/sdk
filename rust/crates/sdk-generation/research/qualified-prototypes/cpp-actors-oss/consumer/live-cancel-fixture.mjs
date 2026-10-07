import { fileURLToPath, pathToFileURL } from "node:url";
import { join } from "node:path";

const sdkRoot = process.env.ACYCLIC_SDK_ROOT;
if (!sdkRoot) throw new Error("ACYCLIC_SDK_ROOT must name the authoritative SDK checkout");
const fixturePath = join(sdkRoot, "typescript", "packages", "actors", "test", "grpc-conformance-fixture.mjs");
const { startConformanceFixture } = await import(pathToFileURL(fixturePath));

let inspectStarted = false;
let inspectAborted = false;
let finish;
const aborted = new Promise(resolve => { finish = resolve; });
const fixture = await startConformanceFixture({
  onUnaryRequest: async (method, _request, context) => {
    if (method.name !== "InspectActor" || inspectStarted) return;
    inspectStarted = true;
    const signal = context.signal;
    if (signal?.aborted) {
      inspectAborted = true;
      finish();
      return;
    }
    signal?.addEventListener("abort", () => {
      inspectAborted = true;
      finish();
    }, { once: true });
    await aborted;
  },
});

process.stdout.write(JSON.stringify({
  endpoint: fixture.options.endpoint,
  token: fixture.options.token,
  caCertificate: fixture.options.caCertificate,
}) + "\n");

const timeout = setTimeout(async () => {
  await fixture.close();
  process.stderr.write(JSON.stringify({ inspectStarted, inspectAborted }) + "\n");
  process.exit(inspectAborted ? 0 : 2);
}, 120000);
timeout.unref();
