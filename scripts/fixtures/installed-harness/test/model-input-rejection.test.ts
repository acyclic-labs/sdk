import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { NativeContracts } from "@acyclic-labs/harness";

type NativeRequest = Parameters<NativeContracts["prepareModelRequest"]>[0];
type NativePolicy = Exclude<Parameters<NativeContracts["prepareModelRequest"]>[2], null>;

type Case = {
  name: string;
  operation: "identity" | "replace" | "remove";
  path?: string;
  value?: unknown;
  context_messages?: number;
  expected: "accept" | "reject";
};

const base = JSON.parse(readFileSync(new URL("../model-input-v3.json", import.meta.url), "utf8")) as {
  limits: Record<string, number>;
  policy: { identity: { name: string; version: string; digest: number[] }; schema: NativePolicy["schema"] };
  root: { request: NativeRequest; expected: { request_json: string } };
};
const matrix = JSON.parse(readFileSync(new URL("../model-input-rejection-v1.json", import.meta.url), "utf8")) as {
  version: number;
  cases: Case[];
};

function editRequest(request: NativeRequest, entry: Case): NativeRequest {
  const edited = structuredClone(request) as unknown as Record<string, unknown>;
  if (entry.operation === "identity") return edited as NativeRequest;
  const path = entry.path?.split("/").slice(1) ?? [];
  let parent: unknown = edited;
  for (const segment of path.slice(0, -1)) {
    if (Array.isArray(parent)) parent = parent[Number(segment)];
    else parent = (parent as Record<string, unknown>)[segment];
  }
  const key = path.at(-1);
  if (key === undefined) throw new Error(`${entry.name}: missing edit path`);
  if (entry.operation === "replace") {
    if (Array.isArray(parent)) parent[Number(key)] = structuredClone(entry.value);
    else (parent as Record<string, unknown>)[key] = structuredClone(entry.value);
  } else if (Array.isArray(parent)) {
    parent.splice(Number(key), 1);
  } else {
    delete (parent as Record<string, unknown>)[key];
  }
  return edited as NativeRequest;
}

function limitsFor(entry: Case) {
  return {
    file_bytes: BigInt(base.limits.file_bytes),
    path_bytes: BigInt(base.limits.path_bytes),
    attachments: BigInt(base.limits.attachments),
    render_bytes: BigInt(base.limits.render_bytes),
    model_steps: BigInt(base.limits.model_steps),
    model_events_per_step: BigInt(base.limits.model_events_per_step),
    tool_calls_per_step: BigInt(base.limits.tool_calls_per_step),
    context_messages: BigInt(entry.context_messages ?? base.limits.context_messages),
  };
}

test("installed Harness exports admit the shared model-input matrix", async () => {
  expect(matrix.version).toBe(1);
  const contracts = await NativeContracts.create();
  const policy = {
    name: base.policy.identity.name,
    version: base.policy.identity.version,
    digest: base.policy.identity.digest,
    schema: base.policy.schema,
  };
  for (const entry of matrix.cases) {
    let admitted = true;
    let requestJson: string | undefined;
    try {
      const result = contracts.prepareModelRequest(
        editRequest(base.root.request, entry),
        limitsFor(entry),
        policy,
      );
      requestJson = result.requestJson;
    } catch {
      admitted = false;
    }
    expect(admitted, entry.name).toBe(entry.expected === "accept");
    if (entry.name === "valid_root") expect(requestJson).toBe(base.root.expected.request_json);
  }
});
