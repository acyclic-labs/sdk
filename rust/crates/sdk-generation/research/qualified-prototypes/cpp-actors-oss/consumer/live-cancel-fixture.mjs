import { pathToFileURL } from "node:url";
import { join } from "node:path";
import { writeFile } from "node:fs/promises";

const sdkRoot = process.env.ACYCLIC_SDK_ROOT;
if (!sdkRoot) throw new Error("ACYCLIC_SDK_ROOT must name the authoritative SDK checkout");
const fixturePath = join(sdkRoot, "typescript", "packages", "actors", "test", "grpc-conformance-fixture.mjs");
const { startConformanceFixture } = await import(pathToFileURL(fixturePath));

let inspectStarted = false;
let inspectAborted = false;
let inspectCount = 0;
let finish;
let closeAndExit;
let closed = false;
const aborted = new Promise(resolve => { finish = resolve; });
const fixture = await startConformanceFixture({
  onUnaryRequest: async (method, _request, context) => {
    if (method.name !== "InspectActor" || inspectStarted) return;
    inspectCount += 1;
    if (inspectCount < 2) return;
    inspectStarted = true;
    const signal = context.signal;
    if (signal?.aborted) {
      inspectAborted = true;
      finish();
      void closeAndExit();
      return;
    }
    signal?.addEventListener("abort", () => {
      inspectAborted = true;
      finish();
      void closeAndExit();
    }, { once: true });
    await aborted;
  },
});

closeAndExit = async () => {
  if (closed) return;
  closed = true;
  await fixture.close();
  process.stderr.write(JSON.stringify({ inspectStarted, inspectAborted }) + "\n");
  process.exit(inspectAborted ? 0 : 2);
};

process.stdout.write(JSON.stringify({
  endpoint: fixture.options.endpoint,
  token: fixture.options.token,
  caCertificate: fixture.options.caCertificate,
}) + "\n");
if (process.env.ACYCLIC_CA_PATH) {
  await writeFile(process.env.ACYCLIC_CA_PATH, fixture.options.caCertificate);
}

const timeout = setTimeout(async () => {
  await closeAndExit();
}, 120000);
timeout.unref();
