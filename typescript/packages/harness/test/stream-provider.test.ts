import { beforeAll, test } from "bun:test";
import assert from "node:assert/strict";
import "fake-indexeddb/auto";
import { materializeBindings } from "../../workers/src/bindings.js";
import { WasmStream } from "../generated/wasm/acyclic_harness_wasm.js";
import { initializeClientQueries } from "../src/client-query.js";
import { EventStreamLedger, streamLedgerPort, streamProviderPort,
  type EventStorage, type OwnedStreamProviderPort } from "../src/event-host.js";
import { sharedProviderTree } from "./support/stream-provider-contract.js";

beforeAll(initializeClientQueries);

async function ledgerBinding(port: OwnedStreamProviderPort): Promise<OwnedStreamProviderPort> {
  // Disposable metadata; the shared case executes every Stream operation on
  // the original Rust provider, through the shipping event-ledger adapter.
  const values = new Map<string, string>();
  const storage: EventStorage = {
    get: key => values.get(key), put: (key, value) => { values.set(key, value); },
    entries: () => [...values], transaction: operation => operation(),
    flush: async () => {}, alarm: async () => {},
  };
  const unused = async (): Promise<never> => { throw new Error("unexpected Objects operation"); };
  const ledger = new EventStreamLedger(storage, { length: unused, read: unused, put: unused },
    () => port, unused, { commands: 64, journal_bytes: 1048576n, paths: 64, path_bytes: 65536,
      records: 1024, payload_bytes: 1048576, commits: 64, idempotency_results: 64 });
  const binding = { ...streamLedgerPort(ledger, "root"), close: () => port.close() };
  const declaration = { name: "STREAMS", capability: "shared-provider-grant", resource: "streams:/",
    targetAccount: new Uint8Array() };
  const env = await materializeBindings([declaration], {
    namespace(): never { throw new Error("unexpected namespace binding"); },
    service(): never { throw new Error("unexpected service binding"); },
    streams(grant) {
      assert.deepEqual(grant, declaration);
      return binding;
    },
  });
  assert.ok(env.STREAMS === binding);
  return env.STREAMS;
}

test("hosted provider bindings share the original fork and activation cut", async () => {
  await sharedProviderTree(await ledgerBinding(streamProviderPort(new WasmStream())));
  await sharedProviderTree(await ledgerBinding(streamProviderPort(await WasmStream.openBrowser(
    `shared-provider-${crypto.randomUUID()}`, 64, 1048576n))));
});
