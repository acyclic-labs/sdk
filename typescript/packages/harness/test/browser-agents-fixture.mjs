import { fromBinary } from "@bufbuild/protobuf";
import { Codec, ReadResponseSchema, RecordBatchSchema } from "../../stream/generated/proto/stream/v1/stream_pb.js";
import init, { AgentGuest, WasmAgentProviders, WasmAgentRegistry, WasmAgents, WasmReducer, decodeClientJson } from "../generated/wasm/acyclic_harness_wasm.js";

// Test composition only: the real Rust agent host over IndexedDB providers.
// The model is a scripted host callback; scheduling and recovery stay in Rust.
export async function agentsFixture(id, reply, initialize = init) {
  await initialize();
  const owner = new WasmReducer({ kind: "task", id }, "browser-agents", new Uint8Array(32).fill(23), []);
  const registry = new WasmAgentRegistry();
  const requests = [];
  registry.configureModel({ provider: "browser-test", name: "scripted", revision: "1", options: {} },
    async function* (bytes, dispatch, signal, files) {
      const request = JSON.parse(new TextDecoder().decode(bytes));
      const texts = files.map(file => new TextDecoder().decode(file));
      requests.push({ request, dispatch, texts });
      yield* reply({ request, dispatch, texts, signal });
      yield { kind: "completed", metadata: {} };
    }, () => null, modelCapacity, modelTokenCount, { kind: "disabled" });
  const options = {
    filesystem_database: `harness-agents-fs-${id}`, stream_database: `harness-agents-stream-${id}`,
    maximum_object_bytes: 1048576n, filesystem_provider: { namespace: "browser-agents", family: "filesystem", version: "2" },
    stream_limits: { commands: 10000n, journal_bytes: 16777216n, paths: 1000, path_bytes: 262144,
      records: 10000, payload_bytes: 16777216, commits: 10000, idempotency_results: 10000 },
    limits: { file_bytes: 1048576n, path_bytes: 4096n, attachments: 32n, render_bytes: 1048576n,
      model_steps: 16n, model_events_per_step: 64n, tool_calls_per_step: 16n, context_messages: 128n },
    principal: "browser-user",
    // The fixture host explicitly accepts every action its agents take.
    preset: { model: null, prompt: null, tools: [], web_tools: false, max_steps: null, max_children: null, approval_policy: { approvers: [{ kind: "rules", principal: "browser-user", rules: [], fallback: "allow" }] } },
  };
  const failures = [];
  const active = new Set();
  return { owner, registry, requests, failures,
    open: () => WasmAgents.open(options, owner, registry, (actor, error) => failures.push({ actor, error })),
    // Host-injected storage, the guest artifact's entry over it, and a host
    // over the same storage whose turns run only through the guest.
    providers: kind => kind === "memory" ? WasmAgentProviders.memory(options) : WasmAgentProviders.indexedDb(options),
    // The fixture holds `owner`'s custody: its guest is the tree's root.
    guest: providers => AgentGuest.open(providers, options, owner, registry, { kind: "root" }),
    hosting: (providers, guest) => WasmAgents.openHosting(providers, options, owner, guest.manifest(),
      (delivery, signal, interrupt) => {
        const result = guest.onRecord(delivery, signal, interrupt);
        active.add(result);
        void result.then(() => active.delete(result), () => active.delete(result));
        return result;
      }, (actor, error) => failures.push({ actor, error })),
    // Host cancellation starts retirement; each guest callback's returned
    // promise fences the borrowed WASM handle before its caller frees it.
    close: async agents => { agents?.free(); await Promise.allSettled(active); },
    free: () => { registry.free(); owner.free(); } };
}

// Synthetic provider units are canonical request bytes, counted once as fixed
// cost; individual message costs are already included in those exact bytes.
export const modelCapacity = () => ({ context_tokens: 131072, output_tokens: 8192 });
export const modelTokenCount = (bytes, request_digest) => ({
  request_digest, input_tokens: BigInt(bytes.byteLength), fixed_tokens: bytes.byteLength,
  message_tokens: JSON.parse(new TextDecoder().decode(bytes)).messages.map(() => 0),
});

// One atomic replay-to-live subscription; no state rechecks or timer.
// The shared Rust cursor produces uncompressed canonical record batches.
export async function until(agents, agent, stream, test) {
  const cursor = await agents.follow(agent, stream, 0n);
  const records = [];
  try {
    for (;;) {
      const bytes = await cursor.next();
      if (bytes === null) throw new Error(`${stream} ended before its condition`);
      const frame = fromBinary(ReadResponseSchema, bytes);
      if (frame.codec !== Codec.NONE || frame.decodedLength !== BigInt(frame.data.length)) {
        throw new Error("agent follow returned a malformed canonical frame");
      }
      for (const record of fromBinary(RecordBatchSchema, frame.data).records) {
        records.push({ sequence: record.sequence, value: decodeClientJson(record.value) });
        if (test(records)) return records;
      }
    }
  } finally {
    cursor.close();
    cursor.free();
  }
}

// The staged output text of `agent`'s settled turn `entry`.
export async function turnText(agents, agent, entry) {
  return JSON.parse(new TextDecoder().decode(await agents.readFile(agent, entry.value.output))).text;
}
