import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  DEFAULT_LIMITS, Harness, NativeContracts, descriptorFor,
  type AgentId, type FileRef, type ForkRequest, type NativeLimitsWire,
} from "../src/index.js";
import {
  digestCanonicalJson, encodeCanonicalJson, prepareModelRequest, validateContract,
} from "../generated/wasm/acyclic_harness_wasm.js";
import type {
  WasmFileRefWire, WasmModelLimitsInput, WasmModelMessageWire, WasmModelRequestWire,
} from "../generated/wasm/acyclic_harness_wasm.js";

const fixture = JSON.parse(readFileSync(new URL("../../../../conformance/vectors/harness/native-wasm-event-v2.json", import.meta.url), "utf8")) as {
  event_wire_hex: string;
};

test("WASM event bytes match the native cross-language fixture", async () => {
  const authority = { kind: "conversation", id: "conversation-1" } as const;
  const harness = await Harness.create({
    authority,
    issuerId: "test",
    issuerKey: new Uint8Array(32).fill(7),
  });
  const scope = harness.issueScope("root", ["conversation:bind", "conversation:append"]);
  const agent = harness.identity("agent", "04040404-0404-0404-0404-040404040404");
  const content = harness.validateFileRef({
    volume: { provider: { namespace: "fixture", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private", owner: { kind: "agent", id: agent } },
    path: "messages/input.txt", version: "pinned-generation",
    descriptor: harness.fileDescriptor(new TextEncoder().encode("fixture text"), "text/plain"),
    display_name: "input.txt",
  });
  harness.apply({
    authority,
    operation_id: harness.identity("operation", "01010101-0101-0101-0101-010101010101"),
    idempotency_key: "bind-1", expected_revision: 0n, scope, causal_parent: null,
    action: { kind: "bind_conversation", agent },
  });
  const command = {
    authority,
    operation_id: harness.identity("operation", "02020202-0202-0202-0202-020202020202"),
    idempotency_key: "append-2",
    expected_revision: 1n,
    scope,
    causal_parent: null,
    action: {
      kind: "append_conversation_message" as const,
      message: { id: harness.conversationMessageId("03030303-0303-0303-0303-030303030303"), sequence: 1,
        kind: "user", content, attachments: { kind: "inline", items: [] },
        reply_to: null, tool_call_id: null, extensions: {} },
    },
  };
  const first = harness.apply(command);
  const replayed = harness.apply(command);
  expect(first.result).toBe("applied");
  expect(replayed.result).toBe("replayed");
  expect(first.eventWire).toEqual(replayed.eventWire);
  expect(Buffer.from(first.eventWire).toString("hex")).toBe(fixture.event_wire_hex);
  harness.free();
});

const filesystem = { namespace: "fixture", family: "filesystem", version: "2" } as const;
const stream = { namespace: "fixture", family: "stream", version: "2" } as const;
const rootAgent = "04040404-0404-0404-0404-040404040404" as AgentId;

function modelLimits(): WasmModelLimitsInput {
  return {
    file_bytes: BigInt(DEFAULT_LIMITS.file_bytes),
    path_bytes: BigInt(DEFAULT_LIMITS.path_bytes),
    attachments: BigInt(DEFAULT_LIMITS.attachments),
    render_bytes: BigInt(DEFAULT_LIMITS.render_bytes),
    model_steps: BigInt(DEFAULT_LIMITS.model_steps),
    model_events_per_step: BigInt(DEFAULT_LIMITS.model_events_per_step),
    tool_calls_per_step: BigInt(DEFAULT_LIMITS.tool_calls_per_step),
    context_messages: BigInt(DEFAULT_LIMITS.context_messages),
  };
}

function nativeLimits(): NativeLimitsWire {
  return {
    file_bytes: BigInt(DEFAULT_LIMITS.file_bytes),
    path_bytes: BigInt(DEFAULT_LIMITS.path_bytes),
    attachments: BigInt(DEFAULT_LIMITS.attachments),
    render_bytes: BigInt(DEFAULT_LIMITS.render_bytes),
    model_steps: BigInt(DEFAULT_LIMITS.model_steps),
    model_events_per_step: BigInt(DEFAULT_LIMITS.model_events_per_step),
    tool_calls_per_step: BigInt(DEFAULT_LIMITS.tool_calls_per_step),
    context_messages: BigInt(DEFAULT_LIMITS.context_messages),
  };
}

async function modelFile(contracts: NativeContracts, level: string, text: string): Promise<FileRef> {
  return contracts.validate("file_ref", {
    volume: { provider: filesystem, id: `model-${level}`, class: "agent_private", owner: { kind: "agent", id: rootAgent } },
    path: `model/level-${level}.txt`, version: `generation-${level}`,
    descriptor: await descriptorFor(new TextEncoder().encode(text), "text/plain"),
    display_name: `level-${level}.txt`,
  });
}

function wireFile(file: FileRef): WasmFileRefWire {
  return file as unknown as WasmFileRefWire;
}

const tool = {
  name: "lookup", revision: "1", description: "read one pinned child input",
  input_schema: { type: "object", additionalProperties: false, properties: { level: { type: "integer" }, note: { type: "string" } } },
  output_schema: { type: "object", additionalProperties: false, properties: { accepted: { type: "boolean" }, text: { type: "string" }, level: { type: "integer" }, output: { type: "string" } } },
  model_output_schema: { type: "object", additionalProperties: false, properties: { accepted: { type: "boolean" }, text: { type: "string" }, level: { type: "integer" }, output: { type: "string" } } },
} as const;

function rootMessages(file: FileRef): WasmModelMessageWire[] {
  return [
    { role: "system", content: "root\r\npolicy: preserve α and 🦀 exactly" },
    { role: "user", content: [
      { kind: "text", text: "parent input\r\nline two\twith Unicode: café" },
      { kind: "file", file: wireFile(file), policy: "reference" },
    ] },
    { role: "assistant", content: { kind: "tool_call", call_id: "root-lookup", name: "lookup", arguments: { level: 0 } } },
    { role: "tool", content: { kind: "tool_result", call_id: "root-lookup", name: "lookup", value: { accepted: true, text: "résumé\r\n✓" } } },
    { role: "assistant", content: { kind: "text", text: "root complete\r\nkeep this prefix" } },
  ];
}

function childMessages(level: number, file: FileRef): WasmModelMessageWire[] {
  const callId = `child-${level}-lookup`;
  return [
    { role: "user", content: [
      { kind: "text", text: `child ${level} instruction\r\nretain parent bytes; suffix ${level} → next` },
      { kind: "file", file: wireFile(file), policy: "reference" },
    ] },
    { role: "assistant", content: { kind: "tool_call", call_id: callId, name: "lookup", arguments: { level, note: "tool → result" } } },
    { role: "tool", content: { kind: "tool_result", call_id: callId, name: "lookup", value: { accepted: true, level, output: `result ${level}\r\nλ` } } },
    { role: "assistant", content: { kind: "text", text: `child ${level} complete\r\nordered` } },
  ];
}

function request(messages: WasmModelMessageWire[]): WasmModelRequestWire {
  return {
    model: { provider: "fixture", name: "recursive", revision: "1", options: {} },
    messages, tools: [tool], max_output_tokens: 256,
  };
}

function bytes(value: unknown): Uint8Array {
  return encodeCanonicalJson(value);
}

function equalBytes(left: Uint8Array | readonly number[], right: Uint8Array | readonly number[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

test("recursive model requests retain exact parent prefixes through raw WASM and typed contract facade", async () => {
  const contracts = await NativeContracts.create();
  const files = await Promise.all([
    modelFile(contracts, "root", "root attachment α\r\nbytes"),
    modelFile(contracts, "one", "child one attachment 🦀\r\nbytes"),
    modelFile(contracts, "two", "child two attachment λ\r\nbytes"),
    modelFile(contracts, "three", "child three attachment ✓\r\nbytes"),
  ]);
  let current = request(rootMessages(files[0]!));
  const requests: WasmModelRequestWire[] = [current];
  const evidence: { requestJson: string; manifestJson: string; requestDigest: readonly number[] }[] = [];

  for (let level = 0; level < requests.length; level += 1) {
    const raw = prepareModelRequest(current, modelLimits(), null) as {
      request_json: string; manifest_json: string; request_digest: readonly number[];
    };
    const facade = contracts.prepareModelRequest(current, nativeLimits());
    expect(facade.requestJson).toBe(raw.request_json);
    expect(facade.manifestJson).toBe(raw.manifest_json);
    expect(facade.requestDigest).toEqual(raw.request_digest);
    expect(new TextDecoder().decode(bytes(current))).toBe(raw.request_json);
    expect(Array.from(digestCanonicalJson(current))).toEqual(Array.from(raw.request_digest));
    evidence.push({ requestJson: raw.request_json, manifestJson: raw.manifest_json, requestDigest: raw.request_digest });

    const decoded = JSON.parse(raw.request_json) as WasmModelRequestWire;
    expect(decoded.messages).toEqual(current.messages);
    const manifest = JSON.parse(raw.manifest_json) as {
      version: number;
      messages: readonly { position: number; role: string; files: readonly WasmFileRefWire[] }[];
    };
    expect(manifest.version).toBe(3);
    expect(manifest.messages.map(message => message.position)).toEqual(decoded.messages.map((_message, position) => position));
    expect(manifest.messages.flatMap(message => message.files).length).toBeGreaterThan(0);
    expect(raw.manifest_json).toContain(`level-${["root", "one", "two", "three"][level]}.txt`);

    if (level === 3) break;
    const parent = current;
    current = request([...parent.messages, ...childMessages(level + 1, files[level + 1]!)]) as WasmModelRequestWire;
    requests.push(current);
    const parentMessages = JSON.parse(evidence[level]!.requestJson) as WasmModelRequestWire;
    const childMessagesDecoded = JSON.parse(prepareModelRequest(current, modelLimits(), null).request_json) as WasmModelRequestWire;
    expect(bytes(childMessagesDecoded.messages.slice(0, parent.messages.length))).toEqual(bytes(parentMessages.messages));
    expect(childMessagesDecoded.messages.slice(0, parent.messages.length)).toEqual(parent.messages);
  }

  expect(requests).toHaveLength(4);
  expect(evidence[3]!.requestDigest).not.toEqual(evidence[0]!.requestDigest);
  const replay = prepareModelRequest(requests[3]!, modelLimits(), null) as {
    request_json: string; manifest_json: string; request_digest: readonly number[];
  };
  expect(replay.request_json).toBe(evidence[3]!.requestJson);
  expect(replay.manifest_json).toBe(evidence[3]!.manifestJson);
  expect(replay.request_digest).toEqual(evidence[3]!.requestDigest);

  const sibling = request([...requests[0]!.messages, ...childMessages(9, files[1]!) ]);
  const siblingRaw = prepareModelRequest(sibling, modelLimits(), null) as { request_json: string };
  const siblingDecoded = JSON.parse(siblingRaw.request_json) as WasmModelRequestWire;
  expect(bytes(siblingDecoded.messages.slice(0, requests[0]!.messages.length))).toEqual(bytes(requests[0]!.messages));
  expect(siblingRaw.request_json).not.toBe(evidence[1]!.requestJson);
});

interface ModelBoundaryWire {
  readonly model: { readonly provider: string; readonly name: string; readonly revision: string; readonly options: Record<string, unknown> };
  readonly publication: string;
  readonly publication_digest: readonly number[];
  readonly boundary_digest: readonly number[];
  readonly attestation: readonly number[];
  readonly files: readonly WasmFileRefWire[];
}

type ForkRequestWithModelBoundary = ForkRequest & { readonly model_boundary: ModelBoundaryWire };

function modelBoundaryRequest(files: readonly FileRef[]): ForkRequestWithModelBoundary {
  const childAgent = "22222222-2222-2222-2222-222222222222" as AgentId;
  const parent = { kind: "conversation", id: "parent" } as const;
  const child = { kind: "conversation", id: "child" } as const;
  return {
    operation_id: "11111111-1111-4111-8111-111111111111" as ForkRequest["operation_id"],
    parent, parent_revision: 3n, child, child_agent: childAgent, attached_agents: [],
    preparation: {
      child_project_volume: { provider: filesystem, id: "child-project", class: "project", owner: { kind: "project", id: "project" } },
      child_private_volume: { provider: filesystem, id: "child-private", class: "agent_private", owner: { kind: "agent", id: childAgent } },
      inherited_through_sequence: 4n, maximum_inherited_messages: 16n,
      maximum_inherited_bytes: 65_536n, maximum_inherited_references: 32,
    },
    selections: [
      { required: true, revision: { kind: "history", reference: { kind: "stream", provider: stream, key: [...new TextEncoder().encode("harness/v2/conversations/parent")], version: "3" } } },
      { required: true, revision: { kind: "project", reference: {
        volume: { provider: filesystem, id: "parent-project", class: "project", owner: { kind: "project", id: "project" } },
        generation: { kind: "generation", provider: filesystem, key: [2], version: null },
      } } },
    ],
    boundary: null,
    model_boundary: {
      model: { provider: "fixture", name: "recursive", revision: "1", options: {} },
      publication: "33333333-3333-4333-8333-333333333333",
      publication_digest: Array(32).fill(1), boundary_digest: Array(32).fill(2), attestation: Array(32).fill(3),
      files: files.map(wireFile),
    },
  };
}

test("nonnull model-boundary envelopes survive raw WASM and typed contract admission", async () => {
  const contracts = await NativeContracts.create();
  const files = [
    await modelFile(contracts, "boundary-root", "boundary root\r\nα"),
    await modelFile(contracts, "boundary-child", "boundary child\r\n🦀"),
  ];
  const request = modelBoundaryRequest(files);
  const raw = validateContract("fork_request", request, null) as ForkRequestWithModelBoundary;
  const facade = contracts.validate("fork_request", request as unknown as ForkRequest) as unknown as ForkRequestWithModelBoundary;
  expect(bytes(raw.model_boundary)).toEqual(bytes(request.model_boundary));
  expect(bytes(facade.model_boundary)).toEqual(bytes(request.model_boundary));
  expect(equalBytes(bytes(raw), bytes(facade))).toBe(true);

  const mutations: readonly [string, (boundary: ModelBoundaryWire) => ModelBoundaryWire][] = [
    ["publication digest", boundary => ({ ...boundary, publication_digest: Array(32).fill(0) })],
    ["boundary digest", boundary => ({ ...boundary, boundary_digest: Array(32).fill(0) })],
    ["attestation", boundary => ({ ...boundary, attestation: Array(32).fill(0) })],
    ["duplicate exact file", boundary => ({ ...boundary, files: [...boundary.files, boundary.files[0]!] })],
    ["invalid publication", boundary => ({ ...boundary, publication: "not-an-operation" })],
    ["path escape", boundary => ({ ...boundary, files: [{ ...boundary.files[0]!, path: "../escape" }, boundary.files[1]!] })],
  ];
  for (const [name, mutate] of mutations) {
    const mutated = { ...request, model_boundary: mutate(structuredClone(request.model_boundary)) };
    expect(() => validateContract("fork_request", mutated, null)).toThrow();
    expect(() => contracts.validate("fork_request", mutated as unknown as ForkRequest)).toThrow();
  }
});
