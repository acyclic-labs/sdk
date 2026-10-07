import { writeFile } from "node:fs/promises";
import { startConformanceFixture } from "file:///C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk/typescript/packages/actors/test/grpc-conformance-fixture.mjs";
const optionsPath = "Q:/sdk/work/actors-uniffi-go-current-20261007/context-options-mac-go-20261007.json";
const fixture = await startConformanceFixture();
await writeFile(optionsPath, JSON.stringify(fixture.options));
process.stdout.write(`${JSON.stringify(fixture.options)}\n`);
const stop = async () => { await fixture.close(); process.exit(0); };
process.on("SIGINT", stop); process.on("SIGTERM", stop);
await new Promise(() => {});
