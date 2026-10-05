import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { DEFAULT_LIMITS, NativeContracts } from "@acyclic-labs/harness";

type EventInput = Parameters<NativeContracts["admitModelEvent"]>[0];
type EventState = NonNullable<Parameters<NativeContracts["admitModelEvent"]>[2]>;

const fixture = JSON.parse(readFileSync(
  new URL("../provider-consumption-v1.json", import.meta.url),
  "utf8",
)) as {
  version: number;
  request_capture: {
    model_vector: string;
    case: string;
    boundary: string;
    provider_adapter: string;
    replay: string;
    forbidden_model_fields: string[];
  };
  responses: Array<{
    name: string;
    prefix: Record<string, unknown>[];
    event: Record<string, unknown>;
    canonical_json: string;
    public_canonical_json?: string;
    model_events_per_step?: number;
    expected: "accept" | "reject";
  }>;
};
const model = JSON.parse(readFileSync(
  new URL("../model-input-v3.json", import.meta.url),
  "utf8",
)) as { limits: Record<string, number>; root: { request: unknown; expected: { request_json: string } } };

function containsKey(value: unknown, key: string): boolean {
  if (Array.isArray(value)) return value.some(item => containsKey(item, key));
  if (value === null || typeof value !== "object") return false;
  return Object.entries(value).some(([name, child]) => name === key || containsKey(child, key));
}

function event(value: Record<string, unknown>): EventInput {
  if (value.kind === "tool_call") {
    const { call_id: callId, ...rest } = value;
    return { ...rest, callId } as EventInput;
  }
  return value as EventInput;
}

test("installed Harness consumes the shared provider response fixture", async () => {
  expect(fixture.version).toBe(1);
  const contracts = await NativeContracts.create();
  expect(fixture.request_capture).toEqual({
    model_vector: "model-input-v3.json",
    case: "root",
    boundary: "prepareModelRequest",
    provider_adapter: "not-exposed",
    replay: "byte-equal",
    forbidden_model_fields: ["operation_id", "dispatch_id", "provider_dispatch_context", "transport_metadata"],
  });
  const requestLimits = {
    file_bytes: BigInt(model.limits.file_bytes),
    path_bytes: BigInt(model.limits.path_bytes),
    attachments: BigInt(model.limits.attachments),
    render_bytes: BigInt(model.limits.render_bytes),
    model_steps: BigInt(model.limits.model_steps),
    model_events_per_step: BigInt(model.limits.model_events_per_step),
    tool_calls_per_step: BigInt(model.limits.tool_calls_per_step),
    context_messages: BigInt(model.limits.context_messages),
  };
  const prepared = contracts.prepareModelRequest(model.root.request as Parameters<NativeContracts["prepareModelRequest"]>[0], requestLimits, null);
  const replayed = contracts.prepareModelRequest(model.root.request as Parameters<NativeContracts["prepareModelRequest"]>[0], requestLimits, null);
  expect(prepared.requestJson).toBe(model.root.expected.request_json);
  expect(replayed.requestJson).toBe(prepared.requestJson);
  expect(replayed.manifestJson).toBe(prepared.manifestJson);
  expect(replayed.requestDigest).toEqual(prepared.requestDigest);
  for (const field of fixture.request_capture.forbidden_model_fields) {
    expect(containsKey(JSON.parse(prepared.requestJson), field), field).toBe(false);
  }
  for (const entry of fixture.responses) {
    const limits = {
      ...DEFAULT_LIMITS,
      ...(entry.model_events_per_step === undefined
        ? {}
        : { model_events_per_step: entry.model_events_per_step }),
    };
    let state: EventState | undefined;
    for (const prefix of entry.prefix) {
      state = contracts.admitModelEvent(event(prefix), limits, state).state;
    }
    const input = event(entry.event);
    const canonical = new TextDecoder().decode(contracts.encodeCanonicalJson(input));
    expect(canonical, entry.name).toBe(entry.public_canonical_json ?? entry.canonical_json);
    let admitted = true;
    try {
      contracts.admitModelEvent(input, limits, state);
    } catch {
      admitted = false;
    }
    expect(admitted, entry.name).toBe(entry.expected === "accept");
  }
});
