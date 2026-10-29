import init, { AgentGuest, WasmAgentProviders, WasmAgentRegistry, WasmAgents, WasmAgentAuthority, decodeReadResponse } from "../generated/wasm/acyclic_harness_wasm.js";
import { fromBinary } from "@bufbuild/protobuf";
import { ReadResponseSchema, RecordBatchSchema } from "../../stream/generated/proto/stream/v1/stream_pb.js";
import { fixtureAuthority, initializeFixtureAuthority } from "../../../../rust/crates/conformance/cloudflare/fixture-authority.mjs";

// Test composition only: the real Rust agent host over IndexedDB providers.
// The model is a scripted host callback; scheduling and recovery stay in Rust.
export async function agentsFixture(id, reply, initialize = init) {
  await initialize();
  await initializeFixtureAuthority();
  const authority = fixtureAuthority(id);
  const owner = new WasmAgentAuthority(authority.capability);
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
    preset: { model: null, prompt: null, tools: [], web_tools: false, max_steps: null, max_children: 8, approval_policy: { approvers: [{ kind: "rules", principal: "browser-user", rules: [], fallback: "allow" }] } },
  };
  const failures = [];
  let rejectFailure;
  const failed = new Promise((_, reject) => { rejectFailure = reject; });
  failed.catch(() => {});
  const report = (actor, error) => { failures.push({ actor, error }); rejectFailure(new Error(`agent ${actor} failed: ${error}`)); };
  return { owner, registry, requests, failures, options,
    failed,
    open: () => WasmAgents.open(options, owner, registry, report),
    // Host-injected storage, the guest artifact's entry over it, and a host
    // over the same storage whose turns run only through the guest.
    providers: kind => kind === "memory" ? WasmAgentProviders.memory(options) : WasmAgentProviders.indexedDb(options),
    guest: providers => AgentGuest.open(providers, options, owner, registry),
    hosting: (providers, guest) => WasmAgents.openHosting(providers, options, owner, guest.manifest(),
      (delivery, signal, interrupt) => guest.onRecord(delivery, signal, interrupt), report),
    free: () => { rejectFailure(new Error("fixture owner closed")); authority.close(); registry.free(); owner.free(); authority.free(); } };
}

// Synthetic provider units are canonical request bytes, counted once as fixed
// cost; individual message costs are already included in those exact bytes.
export const modelCapacity = () => ({ context_tokens: 131072, output_tokens: 8192 });
export const modelTokenCount = (bytes, request_digest) => ({
  request_digest, input_tokens: BigInt(bytes.byteLength), fixed_tokens: bytes.byteLength,
  message_tokens: JSON.parse(new TextDecoder().decode(bytes)).messages.map(() => 0),
});

// The canonical replay-to-live cursor owns registration and cancellation.
// These fixed fixture histories are retained only for their parallel assertions.
export async function untilRecords(fixture, agents, agent, stream, test) {
  const cursor = await agents.follow(agent, stream, 0n);
  const records = [];
  try {
    for (;;) {
      const bytes = await Promise.race([cursor.next(), fixture.failed]);
      if (bytes === null) throw new Error("agent stream closed before its required record");
      const frame = fromBinary(ReadResponseSchema, bytes);
      const batch = fromBinary(RecordBatchSchema, decodeReadResponse(frame.codec, frame.data, frame.decodedLength));
      for (const record of batch.records) records.push({ sequence: record.sequence, value: JSON.parse(new TextDecoder().decode(record.value)) });
      if (test(records)) return records;
    }
  } finally { cursor.close(); cursor.free(); }
}

// The staged output text of `agent`'s settled turn `entry`.
export async function turnText(agents, agent, entry) {
  return JSON.parse(new TextDecoder().decode(await agents.readFile(agent, entry.value.output))).text;
}
