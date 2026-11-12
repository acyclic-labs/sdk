import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { BinaryWriter, WireType } from "@bufbuild/protobuf/wire";
import { loadWasmModule } from "../../actors/src/client.js";
import { initializeClientQueries } from "../src/client-query.js";
import { AgentGuest, WasmAgentProviders, WasmAgentServer } from "../generated/wasm/acyclic_harness_wasm.js";
import { agentsFixture } from "./browser-agents-fixture.mjs";
import { localSessionAuthority } from "./session-authority.js";

const LD = WireType.LengthDelimited, V = WireType.Varint;
const volume = new Uint8Array(readFileSync(new URL("../../../../rust/codec-fuzz/corpus/protobuf/acyclic.harness.v2.VolumeRef-project", import.meta.url))).subarray(8);
const provider = new BinaryWriter().tag(1, LD).string("acyclic").tag(2, LD).string("machines").tag(3, LD).string("1").finish();
function observation(machine: number) {
  const contract = new BinaryWriter()
    .tag(1, LD).fork().tag(1, V).int32(2).tag(3, LD).bytes(new Uint8Array(32).fill(1)).join()
    .tag(3, LD).fork().tag(1, V).int32(1).join().tag(4, LD).bytes(new Uint8Array(32).fill(1))
    .tag(6, LD).fork().tag(1, V).bool(true).join().tag(7, LD).fork().tag(1, V).int32(1).join()
    .tag(8, LD).bytes(new Uint8Array(32).fill(2)).tag(9, LD).fork().join().tag(10, LD).bytes(volume)
    .tag(11, LD).fork().tag(1, V).int32(1).tag(2, V).int32(1).tag(3, V).int32(1).tag(4, V).int32(1).tag(5, V).int32(1).join()
    .tag(12, V).int32(1).finish();
  return new BinaryWriter().tag(1, LD).fork().tag(1, LD).bytes(new Uint8Array(16).fill(machine)).join()
    .tag(2, V).int32(2).tag(3, LD).bytes(contract).tag(6, V).uint64(1n).tag(7, V).uint64(1n).finish();
}

test("one guest heap admits distinct Machines for two actors and re-admits every delivery attempt", async () => {
  const fixture = await agentsFixture("00112233-4455-6677-8899-aabbccddeeff", async function* () {
    yield { kind: "content", delta: "completed on admitted workspace" };
  }, initializeClientQueries);
  const { authority, credential } = await localSessionAuthority([{ name: "actors:/agents", prefix: false, actions: ["actors/invoke"] }]);
  const codec = await loadWasmModule();
  const attempts = new Map<bigint, number>(), machines = new Map<string, number>();
  const admitted: { request: bigint; machine: number }[] = [];
  let deliveries = 0;
  fixture.registry.bindMachinesWorkspaceSource({ admit(request: bigint) {
    const machine = attempts.get(request);
    if (machine === undefined) throw new Error("workspace admission outside its delivery attempt");
    admitted.push({ request, machine });
    return { provider, observation: observation(machine), open() { throw new Error("unexpected Machines RPC"); } };
  } });
  const providers = WasmAgentProviders.memory(fixture.options);
  const guest = AgentGuest.open(providers, fixture.options, fixture.owner, fixture.registry);
  expect(admitted).toEqual([]);
  const openHost = () => WasmAgentServer.open(providers, fixture.options, fixture.owner, fixture.registry, authority, {
    manifest: guest.manifest(), async onRecord(bytes, signal, interrupt) {
      const delivery = codec.decodeDelivery(bytes);
      const actor = Array.from(delivery.actor).join(",");
      if (!machines.has(actor)) machines.set(actor, machines.size + 1);
      attempts.set(delivery.request, machines.get(actor)!);
      deliveries++;
      try { return await guest.onRecord(bytes, signal, interrupt); }
      finally { attempts.delete(delivery.request); }
    }, policy: () => true, admitted: async () => {},
  });
  const firstHost = openHost(), secondHost = openHost();
  try {
    const first = await firstHost.create("first"), second = await secondHost.create("second");
    for (const [host, agent, id] of [[firstHost, first, "first-turn"], [secondHost, second, "second-turn"], [firstHost, first, "next-attempt"]] as const) {
      await host.command({ kind: "prompt", agent, id, body: "hello" }, credential);
      await host.drain(agent);
    }
    expect(await firstHost.read(first, "conversation", 0n, 16)).toHaveLength(2);
    expect(await secondHost.read(second, "conversation", 0n, 16)).toHaveLength(1);
    expect(new Set(admitted.map(value => value.machine)).size).toBe(2);
    expect(admitted).toHaveLength(deliveries);
    expect(new Set(admitted.map(value => value.request)).size).toBe(deliveries);
    expect(attempts.size).toBe(0);
  } finally {
    firstHost.close(); firstHost.free(); secondHost.close(); secondHost.free(); guest.free(); providers.free(); authority.free(); fixture.free(); credential.fill(0);
  }
});
