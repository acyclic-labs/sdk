import { expect, test } from "bun:test";
import {
  DEFAULT_LIMITS, NativeContracts, descriptorFor, type AgentId, type FileRef,
} from "../src/index.js";
import {
  validateModelContent, validateUserInput,
} from "../generated/wasm/acyclic_harness_wasm.js";

const contracts = await NativeContracts.create();
const agent = "07070707-0707-0707-0707-070707070707" as AgentId;

async function file(): Promise<FileRef> {
  return contracts.validate("file_ref", {
    volume: {
      provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private", owner: { kind: "agent", id: agent },
    },
    path: "messages/input.txt", version: "pinned-generation",
    descriptor: await descriptorFor(new TextEncoder().encode("input"), "text/plain"),
    display_name: "input.txt",
  });
}

test("WASM model validators preserve Rust limits and tool-name checks", async () => {
  const content = await file();
  expect(() => validateModelContent("hello", DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "file", file: content, policy: "reference" }, DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "lookup", arguments: { value: 1 } }, DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "bad name", arguments: {} }, DEFAULT_LIMITS)).toThrow();
  expect(() => validateModelContent({ kind: "text", text: "x".repeat(DEFAULT_LIMITS.render_bytes + 1) }, DEFAULT_LIMITS)).toThrow();
  expect(() => validateModelContent({ kind: "unknown", value: true }, DEFAULT_LIMITS)).toThrow();
});

test("WASM user-input validator rejects empty, tool, and malformed content", async () => {
  const content = await file();
  expect(() => validateUserInput("hello")).not.toThrow();
  expect(() => validateUserInput({ kind: "file", file: content, policy: "native" })).not.toThrow();
  expect(() => validateUserInput("")).toThrow();
  expect(() => validateUserInput([])).toThrow();
  expect(() => validateUserInput({ kind: "tool_result", callId: "call", name: "lookup", value: {} })).toThrow();
  expect(() => validateUserInput({ kind: "file", file: { ...content, path: "../escape" }, policy: "reference" })).toThrow();
});
