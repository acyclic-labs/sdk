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

test("Pi default projection resolves bounded text and native images but leaves references opaque", async () => {
  const imageBytes = Uint8Array.of(1, 2, 3);
  const textBytes = new TextEncoder().encode("file text");
  const image = await attachment("images/chart.png", imageBytes, "image/png");
  const textFile = await attachment("notes/readme.txt", textBytes, "text/plain");
  const pdf = await attachment("reports/brief.pdf", Uint8Array.of(7), "application/pdf");
  const input: ModelRequest = { ...request, messages: [{ role: "user", content: [
    { kind: "text", text: "describe" },
    { kind: "file", file: image, policy: "native" },
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
  expect(projected.messages[0]?.content[1]).toEqual({ type: "image", mediaType: "image/png", bytes: imageBytes });
  expect(projected.messages[0]?.content[2]).toEqual({ type: "text", text: "file text" });
  expect(projected.messages[0]?.content[3]).toMatchObject({ type: "text" });
  expect(reads).toEqual([image.path, textFile.path]);
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

test("Pi default projection fails closed for corrupt, oversized, or unsupported files", async () => {
  const pdf = await attachment("reports/brief.pdf", Uint8Array.of(7), "application/pdf");
  const image = await attachment("images/chart.png", Uint8Array.of(7), "image/png");
  const fileMessage = (policy: "native" | "bounded_full" | "reference"): ModelRequest => ({
    ...request, messages: [{ role: "user", content: [{ kind: "file", file: pdf, policy }] }],
  });
  let read = false;
  await expect(projectPiRequest(fileMessage("native"), { resolveFile: async () => { read = true; return Uint8Array.of(7); } })).rejects.toThrow("unsupported");
  expect(read).toBe(false);
  await expect(projectPiRequest(fileMessage("bounded_full"), { resolveFile: async () => Uint8Array.of(7) })).rejects.toThrow("unsupported");
  const imageMessage: ModelRequest = { ...request, messages: [{ role: "user", content: [{ kind: "file", file: image, policy: "native" }] }] };
  await expect(projectPiRequest(imageMessage)).rejects.toThrow("unavailable");
  await expect(projectPiRequest(imageMessage, { resolveFile: async () => Uint8Array.of(8) })).rejects.toThrow();
  await expect(projectPiRequest(imageMessage, { resolveFile: async () => Uint8Array.of(7), maxResolvedBytes: 0 })).rejects.toThrow("limit");
});
