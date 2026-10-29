import { expect, test } from "bun:test";
import { initializeClientQueries } from "../src/client-query.js";
import { AgentGuest, WasmAgentProviders, WasmAgentServer, WasmSessionAuthority, WasmStream } from "../generated/wasm/acyclic_harness_wasm.js";
import { agentsFixture } from "./browser-agents-fixture.mjs";

test("portable host disposal ends pending storage controls and refuses new work", async () => {
  const id = "00112233-4455-6677-8899-aabbccddeeff";
  const agent = `harness/agents/${"01".repeat(16)}`;
  const fixture = await agentsFixture(id, async function* () {}, initializeClientQueries);
  const keys = await crypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]) as CryptoKeyPair;
  const key = [...new Uint8Array(await crypto.subtle.exportKey("raw", keys.publicKey))];
  const authority = new WasmSessionAuthority(JSON.stringify({ environment: "test", environment_keys: [key],
    account: "account", issuer_keys: [key], minimum_key_epoch: 1, revoked_key_ids: [], revoked_block_ids: [] }),
    "workers:/test", "account");
  const audience = { kind: "task", id };
  const filesystem = `filesystem:${JSON.stringify({ layers: [[{
    actions: ["list", "read", "create", "write", "append", "delete", "rename", "fork", "merge-into", "read-history", "lock", "admin"],
    resource: { lineage_root: null, volume: null, generations: "all", path: "**" },
  }]], expires_at: null })}`;
  const admission = { audience, scope: fixture.owner.issueScope("host", [filesystem], undefined),
    account: [...new TextEncoder().encode("test")], namespace: "agents" };
  const operations = {
    validateAdopt: (host: WasmAgentServer) => host.validateAdopt(agent),
    read: (host: WasmAgentServer) => host.read(agent, "status", 0n, 1),
    pause: (host: WasmAgentServer) => host.pause(agent, true),
    archive: (host: WasmAgentServer) => host.archive(agent, "archive"),
    drain: (host: WasmAgentServer) => host.drain(agent),
    create: (host: WasmAgentServer) => host.create("new"),
  };
  try {
    for (const [name, operation] of Object.entries(operations)) {
      const entered = Promise.withResolvers<void>();
      let calls = 0;
      const wait = () => { ++calls; entered.resolve(); return new Promise<never>(() => {}); };
      const stream = WasmStream.connectProvider({ dispatch: wait, open: wait });
      const objects = { length: wait, read: wait, put: wait };
      const providers = WasmAgentProviders.injected(stream, objects, false, fixture.options);
      const guest = AgentGuest.open(providers, fixture.options, fixture.owner, fixture.registry,
        { kind: "admitted", audience, scope: admission.scope });
      const host = WasmAgentServer.open(providers, fixture.options, fixture.owner, fixture.registry,
        admission, authority, { manifest: guest.manifest(), onRecord: () => { throw new Error("unexpected guest delivery"); },
          policy: () => true, admitted: async () => {} });
      try {
        if (name !== "create") await host.adopt(agent);
        const outcome = operation(host).then(
          () => new Error(`storage control ${name} unexpectedly completed`), error => error);
        await Promise.race([entered.promise, outcome.then(error => { throw error; })]);
        host.close();
        const error = await outcome;
        expect(error).toBeInstanceOf(Error);
        expect(error.code).toBe("unavailable");
        expect(error.message).toContain("unavailable");
        const before = calls;
        await expect(host.create("new")).rejects.toThrow("unavailable");
        await expect(host.adopt(agent)).rejects.toThrow("unavailable");
        await expect(operation(host)).rejects.toThrow("unavailable");
        expect(calls).toBe(before);
      } finally {
        host.close(); host.free(); guest.free(); providers.free(); stream.free();
      }
    }
  } finally { authority.free(); fixture.free(); }
}, 10_000);
