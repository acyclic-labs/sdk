import { expect, test } from "bun:test";
import { SwarmClient, WasmStream, initializeClientQueries, swarmStatusKey } from "../src/client-query.js";
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


test("shared status keys preserve opaque bytes and separate agent namespaces", () => {
  const opaque = new Uint8Array([0, 255, 1]);
  expect(swarmStatusKey("swarm/α", opaque)).toEqual(new Uint8Array([...new TextEncoder().encode("swarm/α"), 0, ...opaque]));
  const separate = (a: Uint8Array, b: Uint8Array) => expect(a).not.toEqual(b);
  separate(swarmStatusKey("swarm/a", opaque), swarmStatusKey("swarm/b", opaque));
  // The old unscoped key fails the same separation contract.
  expect(() => separate(opaque, opaque.slice())).toThrow();
  for (const agent of ["", "swarm/a\0b"]) expect(() => swarmStatusKey(agent, opaque)).toThrow();
  expect(() => swarmStatusKey("swarm/a", new Uint8Array())).toThrow();
});
