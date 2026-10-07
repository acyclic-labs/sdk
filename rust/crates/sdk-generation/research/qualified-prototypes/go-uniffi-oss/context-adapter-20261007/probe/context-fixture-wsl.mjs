import { access, writeFile } from "node:fs/promises";
import { startConformanceFixture } from "file:///C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk/typescript/packages/actors/test/grpc-conformance-fixture-wsl.mjs";
const releasePath = "Q:/sdk/work/actors-uniffi-go-current-20261007/release-context-wsl-20261007";
const optionsPath = "Q:/sdk/work/actors-uniffi-go-current-20261007/context-options-wsl-20261007.json";
let pending = true;
const fixture = await startConformanceFixture({
  onUnaryRequest: async method => {
    if (method.name !== "InspectActor") return;
    while (pending) {
      try { await access(releasePath); pending = false; } catch { await new Promise(resolve => setTimeout(resolve, 50)); }
    }
  },
});
await writeFile(optionsPath, JSON.stringify(fixture.options));
process.stdout.write(`${JSON.stringify(fixture.options)}\n`);
const stop = async () => { pending = false; await fixture.close(); process.exit(0); };
process.on("SIGINT", stop); process.on("SIGTERM", stop);
while (pending) await new Promise(resolve => setTimeout(resolve, 100));
await fixture.close();
