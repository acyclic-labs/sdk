import { beforeAll, expect, test } from "bun:test";
import { materializeBindings } from "../../workers/src/bindings.js";
import { EventStreamLedger, streamLedgerPort, streamProviderPort, WasmStream,
  type EventStorage, type ImmutableObjects, type StreamLedger } from "@acyclic-labs/harness/hosting";
import { initializeClientQueries } from "../../harness/src/client-query.js";
import { sharedProviderTree } from "../../harness/test/support/stream-provider-contract.js";
import { HARNESS_BINDINGS } from "../src/bindings.js";
import { harnessStreamLedger } from "../src/streams.js";

beforeAll(initializeClientQueries);

test("Actor bindings retain the original shared ledger and refuse a port without placement", async () => {
  const values = new Map<string, string>();
  const storage: EventStorage = {
    get: key => values.get(key), put: (key, value) => { values.set(key, value); },
    entries: () => [...values], transaction: action => action(),
    flush: async () => {}, alarm: async () => {},
  };
  const unexpected = async (): Promise<never> => { throw new Error("unexpected Objects or wake operation"); };
  const objects: ImmutableObjects = { length: unexpected, read: unexpected, put: unexpected };
  const provider = streamProviderPort(new WasmStream());
  const shared = new EventStreamLedger(storage, objects, () => provider, unexpected,
    { commands: 64, journal_bytes: 1048576n, paths: 64, path_bytes: 65536,
      records: 1024, payload_bytes: 1048576, commits: 64, idempotency_results: 64 });
  try {
    // A transport-only port has no original placement owner. This refusal
    // precedes registration, publication and any attempt to create an Actor.
    expect(() => harnessStreamLedger(provider as never, objects)).toThrow("shared placement authority");
    const declaration = { name: HARNESS_BINDINGS.streams, capability: "shared-provider-grant", resource: "streams:/",
      targetAccount: new Uint8Array(), stateTransaction: false };
    const env = await materializeBindings<never, never, never, Pick<StreamLedger, "register" | "dispatch" | "open">>([declaration], {
      namespace(): never { throw new Error("unexpected namespace binding"); },
      service(): never { throw new Error("unexpected service binding"); },
      streams(grant) { expect(grant).toEqual(declaration); return shared; },
    });
    const binding = env[HARNESS_BINDINGS.streams];
    expect(binding).toBe(shared);
    if (!binding || !("register" in binding)) throw new Error("shared ledger binding missing");
    const ledger = harnessStreamLedger(binding, objects);
    expect(await ledger.register("root", ["tree/root/inbox", "tree/root/control"])).toEqual({ admitted: true });
    expect(await ledger.register("child", ["tree/child/inbox", "tree/child/control"])).toEqual({ admitted: true });
    expect(await ledger.register("other", ["tree/child/inbox", "tree/other/control"])).toEqual({ refused: "conflict" });
    await sharedProviderTree({ ...streamLedgerPort(ledger, "root"), close: () => provider.close() });
  } finally { provider.close(); }
});
