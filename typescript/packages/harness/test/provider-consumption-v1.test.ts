import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { NativeContracts } from "../src/native-contracts.js";
import type { ModelEvent } from "../src/model.js";
import { prepareModelRequest as prepareGeneratedModelRequest } from "../generated/wasm/acyclic_harness_wasm.js";

type EventInput = Parameters<NativeContracts["admitModelEvent"]>[0];
type Limits = Parameters<NativeContracts["admitModelEvent"]>[1];
type EventState = NonNullable<Parameters<NativeContracts["admitModelEvent"]>[2]>;

const base = JSON.parse(readFileSync(
  new URL("../../../../conformance/vectors/harness/model-input-v3.json", import.meta.url),
  "utf8",
)) as {
  limits: Record<string, number>;
  root: { request: unknown; expected: { request_json: string } };
};
const fixture = JSON.parse(readFileSync(
  new URL("../../../../conformance/vectors/harness/provider-consumption-v1.json", import.meta.url),
  "utf8",
)) as {
  version: number;
  responses: Array<{
    name: string;
    prefix: Record<string, unknown>[];
    event: Record<string, unknown>;
    canonical_json: string;
    public_canonical_json?: string;
    model_events_per_step?: number;
    expected: "accept" | "reject";
  }>;
  request_capture: {
    model_vector: string;
    case: string;
    boundary: string;
    provider_adapter: string;
    replay: string;
    forbidden_model_fields: string[];
  };
};

function event(value: Record<string, unknown>): EventInput {
  if (value.kind === "tool_call") {
    const { call_id: callId, ...rest } = value;
    return { ...rest, callId } as EventInput;
  }
  return value as EventInput;
}

function limitsFor(modelEventsPerStep?: number): Limits {
  return {
    file_bytes: base.limits.file_bytes,
    path_bytes: base.limits.path_bytes,
    attachments: base.limits.attachments,
    render_bytes: base.limits.render_bytes,
    model_steps: base.limits.model_steps,
    model_events_per_step: modelEventsPerStep ?? base.limits.model_events_per_step,
    tool_calls_per_step: base.limits.tool_calls_per_step,
    context_messages: base.limits.context_messages,
  };
}

function canonicalText(contracts: NativeContracts, value: ModelEvent): string {
  return new TextDecoder().decode(contracts.encodeCanonicalJson(value));
}

function containsKey(value: unknown, key: string): boolean {
  if (Array.isArray(value)) return value.some(item => containsKey(item, key));
  if (value === null || typeof value !== "object") return false;
  return Object.entries(value).some(([name, child]) => name === key || containsKey(child, key));
}

test("WASM provider request and response consumption matches shared fixture", async () => {
  expect(fixture.version).toBe(1);
  expect(fixture.request_capture).toEqual({
    model_vector: "model-input-v3.json",
    case: "root",
    boundary: "prepareModelRequest",
    provider_adapter: "not-exposed",
    replay: "byte-equal",
    forbidden_model_fields: ["operation_id", "dispatch_id", "provider_dispatch_context", "transport_metadata"],
  });
  const contracts = await NativeContracts.create();
  const requestLimits = {
    file_bytes: BigInt(base.limits.file_bytes),
    path_bytes: BigInt(base.limits.path_bytes),
    attachments: BigInt(base.limits.attachments),
    render_bytes: BigInt(base.limits.render_bytes),
    model_steps: BigInt(base.limits.model_steps),
    model_events_per_step: BigInt(base.limits.model_events_per_step),
    tool_calls_per_step: BigInt(base.limits.tool_calls_per_step),
    context_messages: BigInt(base.limits.context_messages),
  };
  const prepared = contracts.prepareModelRequest(
    base.root.request as Parameters<NativeContracts["prepareModelRequest"]>[0],
    requestLimits,
    null,
  );
  expect(prepared.requestJson).toBe(base.root.expected.request_json);
  const generated = prepareGeneratedModelRequest(
    base.root.request as Parameters<typeof prepareGeneratedModelRequest>[0],
    requestLimits,
    null,
  ) as { request_json: string; manifest_json: string; request_digest: readonly number[] };
  const replayed = prepareGeneratedModelRequest(
    base.root.request as Parameters<typeof prepareGeneratedModelRequest>[0],
    requestLimits,
    null,
  ) as { request_json: string; manifest_json: string; request_digest: readonly number[] };
  expect(generated.request_json).toBe(base.root.expected.request_json);
  expect(replayed.request_json).toBe(generated.request_json);
  expect(replayed.manifest_json).toBe(generated.manifest_json);
  expect(replayed.request_digest).toEqual(generated.request_digest);
  for (const field of fixture.request_capture.forbidden_model_fields) {
    expect(containsKey(JSON.parse(generated.request_json), field), field).toBe(false);
  }
  for (const entry of fixture.responses) {
    const limits = limitsFor(entry.model_events_per_step);
    let state: EventState | undefined;
    for (const prefix of entry.prefix) {
      const admitted = contracts.admitModelEvent(event(prefix), limits, state);
      state = admitted.state;
    }
    const input = event(entry.event);
    const publicEvent = input as ModelEvent;
    expect(canonicalText(contracts, publicEvent), entry.name).toBe(
      entry.public_canonical_json ?? entry.canonical_json,
    );
    let admitted = true;
    try {
      contracts.admitModelEvent(input, limits, state);
    } catch {
      admitted = false;
    }
    expect(admitted, entry.name).toBe(entry.expected === "accept");
  }
});
