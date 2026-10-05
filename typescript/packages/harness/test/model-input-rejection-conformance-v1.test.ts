import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { NativeContracts } from "../src/native-contracts.js";
import type { NativeLimitsWire, NativeModelOptionPolicyWire } from "../src/native-contracts.js";
import type { WasmModelRequestWire } from "../generated/wasm/acyclic_harness_wasm.js";

const baseVector = JSON.parse(readFileSync(
  new URL("../../../../conformance/vectors/harness/model-input-v3.json", import.meta.url),
  "utf8",
)) as {
  limits: Record<keyof NativeLimitsWire, number>;
  policy: { identity: { name: string; version: string; digest: number[] }; schema: NativeModelOptionPolicyWire["schema"] };
  root: { request: WasmModelRequestWire };
};
const matrix = JSON.parse(readFileSync(
  new URL("../../../../conformance/vectors/harness/model-input-rejection-v1.json", import.meta.url),
  "utf8",
)) as {
  version: number;
  cases: Array<{
    name: string;
    operation: "identity" | "replace" | "remove";
    path?: string;
    value?: unknown;
    context_messages?: number;
    expected: "accept" | "reject";
  }>;
};

const editRequest = (request: WasmModelRequestWire, entry: (typeof matrix.cases)[number]): WasmModelRequestWire => {
  const edited = structuredClone(request) as unknown as Record<string, unknown>;
  if (entry.operation === "identity") return edited as unknown as WasmModelRequestWire;
  const path = entry.path?.split("/").slice(1) ?? [];
  let parent: unknown = edited;
  for (const segment of path.slice(0, -1)) {
    parent = (parent as Record<string, unknown>)[segment] ?? (parent as unknown[])[Number(segment)];
  }
  const key = path.at(-1);
  if (key === undefined) throw new Error(`${entry.name}: missing edit path`);
  if (entry.operation === "replace") {
    (parent as Record<string, unknown>)[key] = structuredClone(entry.value);
  } else if (Array.isArray(parent)) {
    parent.splice(Number(key), 1);
  } else {
    delete (parent as Record<string, unknown>)[key];
  }
  return edited as unknown as WasmModelRequestWire;
};

const limitsFor = (entry: (typeof matrix.cases)[number]): NativeLimitsWire => ({
  file_bytes: BigInt(baseVector.limits.file_bytes),
  path_bytes: BigInt(baseVector.limits.path_bytes),
  attachments: BigInt(baseVector.limits.attachments),
  render_bytes: BigInt(baseVector.limits.render_bytes),
  model_steps: BigInt(baseVector.limits.model_steps),
  model_events_per_step: BigInt(baseVector.limits.model_events_per_step),
  tool_calls_per_step: BigInt(baseVector.limits.tool_calls_per_step),
  context_messages: BigInt(entry.context_messages ?? baseVector.limits.context_messages),
});

test("WASM admission matches the shared malformed model-input matrix", async () => {
  expect(matrix.version).toBe(1);
  const contracts = await NativeContracts.create();
  const policy: NativeModelOptionPolicyWire = {
    name: baseVector.policy.identity.name,
    version: baseVector.policy.identity.version,
    digest: baseVector.policy.identity.digest,
    schema: baseVector.policy.schema,
  };
  for (const entry of matrix.cases) {
    let admitted = true;
    try {
      contracts.prepareModelRequest(editRequest(baseVector.root.request, entry), limitsFor(entry), policy);
    } catch {
      admitted = false;
    }
    expect(admitted, entry.name).toBe(entry.expected === "accept");
  }
});
