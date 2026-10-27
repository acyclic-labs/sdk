import { configurePiModel, type PiBridge, type PiModelEvent } from "../src/index.js";
import type { BrowserAgentRegistry } from "@acyclic-labs/harness/browser";

type RegistryParameters = Parameters<BrowserAgentRegistry["configureModel"]>;

declare const registry: BrowserAgentRegistry;
declare const model: RegistryParameters[0];
declare const compaction: RegistryParameters[5];
declare const capacity: NonNullable<RegistryParameters[3]>;
declare const count: NonNullable<RegistryParameters[4]>;

const bridge: PiBridge<{bytes: Uint8Array}, {event: PiModelEvent}> = {
  contextCapacity: capacity,
  countTokens: count,
  project: (bytes, files) => { void files; return {bytes}; },
  async *run(request, {dispatch, signal}) {
    void request.bytes;
    void dispatch.operation_id;
    void dispatch.request_digest;
    signal.throwIfAborted();
  },
  event: event => event.event,
  reconcile: async attempt => { void attempt.observed; return null; },
};
configurePiModel(registry, model, compaction, bridge);
