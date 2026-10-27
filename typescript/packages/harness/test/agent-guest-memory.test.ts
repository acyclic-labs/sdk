import { expect, test } from "bun:test";
import { ensureBrowserWasm } from "../src/wasm-runtime.js";
// @ts-expect-error The shared browser fixture is untyped JavaScript.
import { agentsFixture, turnText, until } from "./browser-agents-fixture.mjs";

// A guest is a pure handler over host-injected storage: here memory, with no
// IndexedDB global at all, as in a server-side isolate.
test("the guest artifact runs one agent turn over in-memory host storage", async () => {
  // Other suites in this process may install fake-indexeddb; hide it here.
  const global = globalThis as { indexedDB?: unknown };
  const installed = Object.getOwnPropertyDescriptor(global, "indexedDB");
  delete global.indexedDB;
  // The fixture is untyped JavaScript, so its handles are too.
  let fixture: any, providers: any, guest: any, agents: any;
  try {
    fixture = await agentsFixture(crypto.randomUUID(), async function* ({ texts }: { texts: string[] }) {
      yield { kind: "content", delta: `guest:${texts.at(-1)}` };
    }, ensureBrowserWasm);
    expect("indexedDB" in globalThis).toBe(false);
    providers = fixture.providers("memory");
    guest = fixture.guest(providers);
    let deliveries = 0;
    agents = fixture.hosting(providers, {
      manifest: () => guest.manifest(),
      onRecord: (delivery: Uint8Array, signal: AbortSignal) => { deliveries += 1; return guest.onRecord(delivery, signal); },
    });
    const agent = await agents.create("agent-guest", "hello");
    const entries = await until(() => agents.read(agent, "conversation", 0n, 16), (entries: unknown[]) => entries.length === 1);
    expect(await turnText(agents, agent, entries[0])).toBe("guest:hello");
    expect(deliveries).toBeGreaterThanOrEqual(1);
    expect(fixture.failures).toEqual([]);
  } finally {
    agents?.free();
    guest?.free();
    providers?.free();
    fixture?.free();
    if (installed !== undefined) Object.defineProperty(global, "indexedDB", installed);
  }
}, 30_000);
