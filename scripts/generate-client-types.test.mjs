import assert from "node:assert/strict";
import test from "node:test";
import { render as renderHttp, rust as httpSource } from "./generate-stream-http-contract.mjs";
import { render as renderTokens } from "./generate-stream-token-operations.mjs";
import { render as renderWidths, rust as widthSource } from "./generate-inference-widths.mjs";

test("Stream HTTP generator keeps route result types without a runtime contract export", () => {
  assert.deepEqual(httpSource, [["acyclic-stream", "bin", "stream-http-contract"]]);
  const source = renderHttp([JSON.stringify([["tail", "tail"], ["read", "read"]])])["typescript/packages/stream/src/http-contract.ts"];
  assert.match(source, /type HttpResponseContract = \{\n  "tail": "tail";\n  "read": "read";\n\};/);
  assert.match(source, /export type HttpRoute = keyof HttpResponseContract;/);
  assert.match(source, /HttpResponseByKind\[HttpResponseContract\[Route\]\]/);
  assert.doesNotMatch(source, /HTTP_RESPONSE_CONTRACT|Object\.freeze|export const/);
});

test("Stream token generation preserves captured authorization rows and derives the operation type", () => {
  // Captured from `bun scripts/generate-contracts.mjs write stream-token-operations`
  // at a473ef01e45218efa1e78e6595404ab17b9c3b4f, using the real Rust exporter.
  const operations = [
    { actions: ["streams/list"], operation: "list" },
    { actions: ["streams/follow", "streams/read"], operation: "read" },
    { actions: ["streams/follow", "streams/read"], operation: "follow" },
    { actions: ["streams/append"], operation: "append" },
    { actions: ["streams/fork", "streams/create"], operation: "fork" },
    { actions: ["streams/create"], operation: "create" },
    { actions: ["streams/commit"], operation: "commit" },
  ];
  const files = renderTokens([JSON.stringify(operations)]);
  assert.deepEqual(Object.keys(files), ["typescript/packages/stream/src/token-operations.ts"]);
  const source = files["typescript/packages/stream/src/token-operations.ts"];
  const declaration = source.match(/export const TOKEN_AUTHORIZATION\s*=\s*([\s\S]*?)\s+as const;/);
  assert.ok(declaration, "token authorization must retain its readonly literal declaration");
  assert.deepEqual(JSON.parse(declaration[1]), operations, "token actions must survive generation unchanged");
  assert.match(source, /export type TokenOperation\s*=\s*typeof TOKEN_AUTHORIZATION\[number\]\["operation"\];/);
  assert.throws(
    () => renderTokens([JSON.stringify([...operations, { ...operations[1], actions: operations[0].actions }])]),
    /Rust Stream token operation inventory contains duplicates/,
  );
});

test("Stream generators retain malformed source inventory rejection", () => {
  for (const value of [null, {}, ["tail"], [["tail", "unknown"]], [[1, "tail"]]]) {
    assert.throws(() => renderHttp([JSON.stringify(value)]), /Rust Stream HTTP contract has an invalid shape/);
  }
  for (const value of [null, {}, [], [""], [1]]) {
    assert.throws(() => renderTokens([JSON.stringify(value)]), /Rust Stream token admission/);
  }
});

test("inference generator retains named Rust widths without publishing the unused raw dump", () => {
  assert.deepEqual(widthSource, [["acyclic-inference-wasm", "example", "inference-typescript-widths"]]);
  const files = renderWidths([JSON.stringify({ runId: 16, contextRevision: 32, futureWidth: 7 })]);
  assert.deepEqual(Object.keys(files).sort(), [
    "typescript/packages/inference/generated/widths.d.ts", "typescript/packages/inference/generated/widths.js",
  ]);
  assert.match(files["typescript/packages/inference/generated/widths.js"], /export const INFERENCE_FIXED_WIDTHS = Object\.freeze\(/);
  assert.match(files["typescript/packages/inference/generated/widths.d.ts"], /readonly "runId": 16;/);
  assert.match(files["typescript/packages/inference/generated/widths.d.ts"], /readonly "contextRevision": 32;/);
  assert.match(files["typescript/packages/inference/generated/widths.d.ts"], /readonly "futureWidth": 7;/);
});

