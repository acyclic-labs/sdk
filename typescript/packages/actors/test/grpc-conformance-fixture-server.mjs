import { startConformanceFixture } from "./grpc-conformance-fixture.mjs";

const fixture = await startConformanceFixture();
let stopping = false;
const stop = async () => {
  if (stopping) return;
  stopping = true;
  await fixture.close();
  process.exit(0);
};

process.once("SIGINT", stop);
process.once("SIGTERM", stop);
process.stdout.write(`${JSON.stringify(fixture.options)}\n`);
