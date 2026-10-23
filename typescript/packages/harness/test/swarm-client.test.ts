import { expect, test } from "bun:test";
import { SwarmClient, WasmStream, initializeClientQueries } from "../src/client-query.js";
import { exerciseSwarm, limits, viewLimits } from "./swarm-client.scenario.js";
import { bindLocalSession } from "./session-authority.js";

test("memory volumes use the production swarm pool and file fold through WASM", async () => {
  await initializeClientQueries();
  const stream = new WasmStream();
  await bindLocalSession(stream);
  try { expect(await exerciseSwarm(stream)).toEqual({ agents: 2, snapshots: 2, textDeltas: 1, recovered: false }); }
  finally { stream.free(); }
});
for (const mutation of [{ agents: 0n }, { selected: 3n }, { snapshot_files: 0n }, { config_files: 0n }, { layout_bytes: 0n }, { extra: true }]) {
  test(`Rust rejects invalid swarm admission before following: ${Object.keys(mutation)}`, async () => {
    await initializeClientQueries();
    expect(() => Reflect.construct(SwarmClient, ["swarm", { ...limits, ...mutation }, viewLimits])).toThrow();
  });
}
