import { expect, test } from "bun:test";
import {
  initializeModelContent, modelContentInventory, fileDescriptor, encodeCanonicalJson,
  type ModelContentInput, type ModelContentLimits,
} from "@acyclic-labs/harness";

const fixtureLimits: ModelContentLimits = { file_bytes: 65_536, path_bytes: 4_096, attachments: 16,
  render_bytes: 65_536, model_steps: 8, model_events_per_step: 64, tool_calls_per_step: 16, context_messages: 128 };

test("installed native inventory preserves ordered tool media, option refs and full-width revisions", async () => {
  await initializeModelContent();
  const agent = "07070707-0707-0707-0707-070707070707";
  const file = {
    volume: { provider: { namespace: "installed", family: "filesystem", version: "2" },
      id: "media", class: "agent_private", owner: { kind: "agent", id: agent } },
    path: "image.bin", version: "immutable", display_name: "image.bin",
    descriptor: fileDescriptor(Uint8Array.of(1), "application/octet-stream"),
  };
  const options = { ...file, path: "options.json", display_name: "options.json",
    descriptor: fileDescriptor(new TextEncoder().encode("{}"), "application/json") };
  const configuration = {
    source: { authority: { kind: "agent", id: agent }, revision: 9_007_199_254_740_993n },
    configuration: { extension: { name: "fixture.options", version: 1 }, schema_digest: Array(32).fill(1), content: options },
    implementation_digest: Array(32).fill(2),
  };
  const part = { kind: "file", file, policy: { native: {
    intent: { kind: "image", detail: "auto" }, maximum_bytes: 64, maximum_work: 8, configuration,
  } } };
  const content = { kind: "tool_result", callId: "call", name: "inspect",
    content: { kind: "parts", parts: Object.freeze([{ kind: "text", text: "media" }, part]) } };
  const inventory = modelContentInventory(content as ModelContentInput, fixtureLimits);
  expect(inventory.files).toEqual([file, options]);
  expect(encodeCanonicalJson(inventory.nativeConfigurations)).toEqual(encodeCanonicalJson([configuration]));
  expect(inventory.nativeConfigurations[0]?.source.revision).toBe(9_007_199_254_740_993n);
  // The removed JS facade froze its copies. Rust inventory is declarative,
  // not an admission of the original option selection or a file read grant.
});
