import { expect, test } from "bun:test";
import { piDefaultProvider, piProvider, projectPiEvent, projectPiRequest, type PiEvent } from "../src/index.ts";
import { descriptorFor, NativeContracts, type FileRef, type ModelRequest } from "@acyclic-labs/harness";

const request: ModelRequest = { model: { provider: "pi", name: "x", revision: "1", options: {} }, messages: [{ role: "user", content: "hello" }], tools: [] };
test("Pi bridge receives the typed request and preserves terminal metadata", async () => {
  const events: readonly PiEvent<{ readonly session: string }>[] = [
    { type: "text_delta", text: "ok" }, { type: "complete", metadata: { session: "s" } },
  ];
  const provider = piProvider({
    project: value => { expect(value).toEqual(request); return value; },
    run: async function* () { yield* events; },
    async reconcile() { return events; },
  });
  const values = [];
  for await (const event of provider.generate(request)) values.push(event);
  expect(values.at(-1)).toEqual({ kind: "completed", metadata: { session: "s" } });
  expect(await provider.reconcile({ operationId: "o", step: 1, requestDigest: new Uint8Array(), observed: [] })).toEqual(values);
});

test("Pi bridge rejects unknown or malformed events explicitly", () => {
  expect(() => projectPiEvent({ type: "unknown" })).toThrow(TypeError);
  expect(() => projectPiEvent({ type: "tool_call", id: "", name: "tool", arguments: {} })).toThrow(TypeError);
  expect(() => projectPiEvent({ type: "complete" })).toThrow("metadata is missing");
});

async function attachment(path: string, bytes: Uint8Array, mediaType: string): Promise<FileRef> {
  const contracts = await NativeContracts.create();
  const file = {
    volume: { provider: { namespace: "pi-test", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private", owner: { kind: "agent", id: contracts.validateIdentity("agent", "08080808-0808-0808-0808-080808080808") } },
    path, version: "pinned-1", descriptor: await descriptorFor(bytes, mediaType), display_name: path.split("/").at(-1)!,
  } satisfies FileRef<"agent_private">;
  return contracts.validate("file_ref", file);
}

test("Pi default projection resolves bounded text and leaves image and document references opaque", async () => {
  const imageBytes = Uint8Array.of(1, 2, 3);
  const textBytes = new TextEncoder().encode("file text");
  const image = await attachment("images/chart.png", imageBytes, "image/png");
  const textFile = await attachment("notes/readme.txt", textBytes, "text/plain");
  const pdf = await attachment("reports/brief.pdf", Uint8Array.of(7), "application/pdf");
  const input: ModelRequest = { ...request, messages: [{ role: "user", content: [
    { kind: "text", text: "describe" },
    { kind: "file", file: image, policy: "reference" },
    { kind: "file", file: textFile, policy: "bounded_full" },
    { kind: "file", file: pdf, policy: "reference" },
  ] }] };
  const reads: string[] = [];
  const resolveFile = async (file: FileRef) => {
    reads.push(file.path);
    return file.path === image.path ? imageBytes : textBytes;
  };
  const projected = await projectPiRequest(input, { resolveFile, maxResolvedBytes: 64 });
  expect(projected.messages[0]?.content[0]).toEqual({ type: "text", text: "describe" });
  expect(projected.messages[0]?.content[1]).toMatchObject({ type: "text" });
  expect(projected.messages[0]?.content[2]).toEqual({ type: "text", text: "file text" });
  expect(projected.messages[0]?.content[3]).toMatchObject({ type: "text" });
  expect(reads).toEqual([textFile.path]);
  let seen = false;
  const provider = piDefaultProvider({ resolveFile, maxResolvedBytes: 64,
    run: async function* (value) { seen = value.messages[0]?.content.length === 4; yield { type: "complete" as const, metadata: null }; },
    async reconcile() { return undefined; },
  });
  const events = [];
  for await (const event of provider.generate(input)) events.push(event);
  expect(seen).toBe(true);
  expect(events.at(-1)).toEqual({ kind: "completed", metadata: null });
});

test("Pi rejects native media across the request before reading supported files", async () => {
  const image = await attachment("images/chart.png", Uint8Array.of(7), "image/png");
  const text = await attachment("notes/input.txt", new TextEncoder().encode("input"), "text/plain");
  const native = { kind: "file" as const, file: image, policy: { native: {
    intent: { kind: "image" as const, detail: "auto" as const }, maximum_bytes: 64, maximum_work: 8, configuration: null,
  } } };
  for (const content of [native, { kind: "tool_result" as const, callId: "call", name: "inspect",
    content: { kind: "parts" as const, parts: [native] } }]) {
    let reads = 0;
    const input: ModelRequest = { ...request, messages: [
      { role: "user", content: { kind: "file", file: text, policy: "bounded_full" } },
      { role: content.kind === "tool_result" ? "tool" : "user", content },
    ] };
    await expect(projectPiRequest(input, { resolveFile: async () => { reads++; return Uint8Array.of(7); } }))
      .rejects.toThrow("unsupported native media policy");
    expect(reads).toBe(0);
  }
  const input: ModelRequest = { ...request, messages: [{ role: "user", content: { kind: "file", file: text, policy: "bounded_full" } }] };
  await expect(projectPiRequest(input)).rejects.toThrow("unavailable");
  await expect(projectPiRequest(input, { resolveFile: async () => Uint8Array.of(8) })).rejects.toThrow();
  await expect(projectPiRequest(input, { resolveFile: async () => Uint8Array.of(7), maxResolvedBytes: 0 })).rejects.toThrow("limit");
});

test("Pi projects canonical JSON and ordered tool data envelopes", async () => {
  const file = await attachment("notes/input.txt", new TextEncoder().encode("input"), "text/plain");
  const input: ModelRequest = { ...request, messages: [
    { role: "tool", content: { kind: "tool_result", callId: "json", name: "inspect", content: { kind: "json", value: 12 } } },
    { role: "tool", content: { kind: "tool_result", callId: "parts", name: "inspect", content: { kind: "parts", parts: [
      { kind: "text", text: "before" }, { kind: "file", file, policy: "bounded_full" },
    ] } } },
  ] };
  const projected = await projectPiRequest(input, { resolveFile: async () => new TextEncoder().encode("input") });
  expect(projected.messages[0]?.content[0]).toEqual({ type: "tool_result", callId: "json", name: "inspect", value: 12 });
  expect(projected.messages[1]?.content[0]).toEqual({ type: "tool_result", callId: "parts", name: "inspect",
    value: [{ type: "text", text: "before" }, { type: "text", text: "input" }] });
});


test("Pi accounting forwards the exact model and prepared request without dispatch", () => {
  const prepared = { ...request, serializedInput: Uint8Array.of(1, 2, 3) };
  const capacity = { contextTokens: 100_000, outputTokens: 16_384 };
  const count = { requestDigest: new Uint8Array(32), fixedTokens: 3, messageTokens: [5] };
  let projected = 0;
  let dispatched = 0;
  const bridge = {
    contextCapacity(model: ModelRequest["model"]) { expect(model).toBe(request.model); return capacity; },
    countTokens(value: typeof prepared) { expect(value).toBe(prepared); return count; },
    project(value: ModelRequest) { projected++; return value; },
    async *run() { dispatched++; yield { type: "complete" as const, metadata: null }; },
    async reconcile() { return undefined; },
  };
  for (const provider of [piProvider(bridge), piDefaultProvider(bridge)]) {
    expect(provider.contextCapacity(request.model)).toBe(capacity);
    expect(provider.countTokens(prepared)).toBe(count);
  }
  expect(projected).toBe(0);
  expect(dispatched).toBe(0);
});

test("Pi accounting rejects absent capacity and counts without guessing", () => {
  const bridge = {
    project(value: ModelRequest) { return value; },
    async *run() { yield { type: "complete" as const, metadata: null }; },
    async reconcile() { return undefined; },
  };
  for (const provider of [piProvider(bridge), piDefaultProvider(bridge)]) {
    expect(() => provider.contextCapacity(request.model)).toThrow("explicit model capacity accounting");
    expect(() => provider.countTokens({ ...request, serializedInput: new Uint8Array() })).toThrow("explicit token accounting");
  }
});
