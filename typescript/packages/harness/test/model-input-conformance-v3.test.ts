import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { NativeContracts } from "../src/native-contracts.js";
import type { NativeLimitsWire, NativeModelOptionPolicyWire } from "../src/native-contracts.js";
import type { WasmModelRequestWire } from "../generated/wasm/acyclic_harness_wasm.js";

const vector = JSON.parse(readFileSync(
  new URL("../../../../conformance/vectors/harness/model-input-v3.json", import.meta.url),
  "utf8",
)) as {
  version: number;
  limits: Record<string, number>;
  policy: { identity: { name: string; version: string; digest: number[] }; schema: unknown };
  root: Case;
  children: Case[];
  grandchild: Case;
};

interface Case {
  name?: string;
  prefix_message_count: number;
  request: Record<string, unknown>;
  expected: Expected;
}

interface Expected {
  request_json: string;
  manifest_json: string;
  request_digest: number[];
  binding_digest: number[];
  manifest_digest: number[];
  prefix_digest: number[];
}

const bytesEqual = (left: readonly number[] | Uint8Array, right: readonly number[] | Uint8Array): boolean =>
  left.length === right.length && left.every((byte, index) => byte === right[index]);

test("TypeScript consumes the native frozen model-input vector exactly", async () => {
  expect(vector.version).toBe(3);
  const contracts = await NativeContracts.create();
  const limits: NativeLimitsWire = Object.fromEntries(
    Object.entries(vector.limits).map(([key, value]) => [key, BigInt(value)]),
  ) as NativeLimitsWire;
  const policy: NativeModelOptionPolicyWire = {
    name: vector.policy.identity.name,
    version: vector.policy.identity.version,
    digest: vector.policy.identity.digest,
    schema: vector.policy.schema,
  };
  const admit = (entry: Case) => {
    const evidence = contracts.prepareModelRequest(entry.request as WasmModelRequestWire, limits, policy);
    expect(evidence.requestJson).toBe(entry.expected.request_json);
    expect(evidence.manifestJson).toBe(entry.expected.manifest_json);
    expect(evidence.requestDigest).toEqual(entry.expected.request_digest);
    expect([...contracts.digestCanonicalJson(JSON.parse(evidence.requestJson))]).toEqual(entry.expected.request_digest);
    expect([...contracts.digestCanonicalJson(JSON.parse(evidence.manifestJson))]).toEqual(entry.expected.manifest_digest);
    expect([...contracts.digestCanonicalJson([
      3,
      entry.expected.binding_digest,
      JSON.parse(evidence.requestJson).messages
        .slice(0, entry.prefix_message_count)
        .map((message: unknown) => [...contracts.encodeCanonicalJson(message)]),
    ])]).toEqual(entry.expected.prefix_digest);
    return { evidence, request: JSON.parse(evidence.requestJson) as { messages: unknown[] } };
  };

  const root = admit(vector.root);
  const children = vector.children.map(admit);
  for (const child of children) {
    expect(child.request.messages.slice(0, vector.root.prefix_message_count))
      .toEqual(root.request.messages.slice(0, vector.root.prefix_message_count));
  }
  expect(children[0].request.messages.slice(vector.root.prefix_message_count))
    .not.toEqual(children[1].request.messages.slice(vector.root.prefix_message_count));
  const grandchild = admit(vector.grandchild);
  expect(grandchild.request.messages.slice(0, vector.grandchild.prefix_message_count))
    .toEqual(children[0].request.messages.slice(0, vector.grandchild.prefix_message_count));

  // The provider-facing body is the exact admitted request JSON. A transport
  // adapter may attach metadata around it, but cannot change these bytes.
  expect(bytesEqual(
    new TextEncoder().encode(root.evidence.requestJson),
    contracts.encodeCanonicalJson(root.request),
  )).toBe(true);
});
