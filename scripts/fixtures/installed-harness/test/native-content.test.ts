import { expect, test } from "bun:test";
import {
  DEFAULT_LIMITS, Harness, NativeContracts, descriptorFor, type AgentId, type FileRef,
} from "@acyclic-labs/harness";

const contracts = await NativeContracts.create();
const agent = "07070707-0707-0707-0707-070707070707" as AgentId;

async function nativePart() {
  const file: FileRef = contracts.validate("file_ref", {
    volume: { provider: { namespace: "installed", family: "filesystem", version: "2" },
      id: "media", class: "agent_private", owner: { kind: "agent", id: agent } },
    path: "image.bin", version: "immutable", display_name: "image.bin",
    descriptor: await descriptorFor(Uint8Array.of(1), "application/octet-stream"),
  });
  const options = { ...file, path: "options.json", display_name: "options.json",
    descriptor: await descriptorFor(new TextEncoder().encode("{}"), "application/json") };
  return { kind: "file" as const, file, policy: { native: {
    intent: { kind: "image" as const, detail: "auto" as const }, maximum_bytes: 64, maximum_work: 8,
    configuration: {
      source: { authority: { kind: "agent" as const, id: agent }, revision: 9_007_199_254_740_993n },
      configuration: { extension: { name: "fixture.options", version: 1 }, schema_digest: Array(32).fill(1), content: options },
      implementation_digest: Array(32).fill(2),
    },
  } } };
}

test("installed native inventory preserves ordered tool media, option refs and full-width revisions", async () => {
  const part = await nativePart();
  const inventory = contracts.modelContentInventory({ kind: "tool_result", callId: "call", name: "inspect",
    content: { kind: "parts", parts: Object.freeze([{ kind: "text" as const, text: "media" }, part]) } }, DEFAULT_LIMITS);
  expect(inventory.files).toEqual([part.file, part.policy.native.configuration.configuration.content]);
  expect(inventory.nativeConfigurations).toEqual([part.policy.native.configuration]);
  expect(inventory.nativeConfigurations[0]?.source.revision).toBe(9_007_199_254_740_993n);
  expect(Object.isFrozen(inventory.nativeConfigurations[0])).toBe(true);
});

test("installed local runtime rejects unsupported original option admission before provider IO", async () => {
  let calls = 0;
  const runtime = await Harness.builder(contracts).model(
    { provider: "mock", name: "installed-native-options", revision: "pinned", options: {} }, {
      async *generate() { calls++; yield { kind: "completed" as const, metadata: {} }; },
      async reconcile() { return undefined; },
    }).build();
  await expect(runtime.run({ prompt: "", content: [await nativePart()] })).rejects.toThrow(
    "original native option admission is unavailable");
  expect(calls).toBe(0);
});
