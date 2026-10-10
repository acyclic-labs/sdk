// Run in a consumer installed from the assembled package tarball.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const publicUrl = import.meta.resolve("@acyclic-labs/inference");
const root = dirname(dirname(fileURLToPath(publicUrl)));
const { INFERENCE_FIXED_WIDTHS: widths } = await import(pathToFileURL(join(root, "generated/widths.js")).href);
assert.ok(Object.isFrozen(widths));
assert.throws(() => { widths.runId = 32; }, TypeError);
const { contextRevision, runId, warmCommitment, executionProfile, itemId } = await import(publicUrl);
for (const [construct, width] of [[contextRevision, widths.contextRevision], [runId, widths.runId], [warmCommitment, widths.warmCommitment], [executionProfile, widths.executionProfile]]) {
  assert.equal(construct(new Uint8Array(width)).byteLength, width);
  assert.throws(() => construct(new Uint8Array(width - 1)), TypeError);
  assert.throws(() => construct(new Uint8Array(width + 1)), TypeError);
  assert.throws(() => construct(new Array(width).fill(0)), TypeError);
}
assert.equal(itemId(new Uint8Array(0)).byteLength, 0);
console.log(JSON.stringify({ status: "passed", namedWidths: Object.keys(widths).length, publicConstructors: 4, negativeControls: 13 }));

// The installed public route adapter consumes the Rust-generated inventory.
const { deriveInferenceHttpRoutes, validateInferenceHttpPath, RunTerminal } = await import(publicUrl);
const routes = deriveInferenceHttpRoutes();
assert.equal(new Set(routes.map(route => `${route.method.parent.typeName}.${route.method.name}`)).size, routes.length);
assert.equal(routes.find(route => route.method.name === "Watch")?.methodKind, "server_streaming");
validateInferenceHttpPath("Models/model.v2_~");
assert.throws(() => validateInferenceHttpPath("runs/../escape"));
const { RUN_TERMINAL_METADATA: terminals } = await import(pathToFileURL(join(root, "generated/terminal-metadata.js")).href);
assert.ok(Object.isFrozen(terminals) && terminals.every(Object.isFrozen));
assert.throws(() => { terminals[0].partial = true; }, TypeError);

// Compile the actual shipped Rust binary and initialize the public deployment
// module entry point, then exercise its state machine rather than comparing
// generated metadata or declarations with another generated copy.
const { initializeInferenceWasm, watchRunStart, watchRunAdvance, watchRunFinish } =
  await import("@acyclic-labs/inference/wasm");
const { create, toBinary } = await import("@bufbuild/protobuf");
const { RunViewSchema, RunEventSchema } = await import("@acyclic-labs/inference/proto");
const compiled = new WebAssembly.Module(
  await readFile(new URL(import.meta.resolve("@acyclic-labs/inference/module.wasm"))),
);
await initializeInferenceWasm(compiled);
const id = runId(new Uint8Array(widths.runId).fill(1));
const view = create(RunViewSchema, {
  runId: id,
  input: contextRevision(new Uint8Array(widths.contextRevision).fill(2)),
  model: "model",
});
const state = await watchRunStart(toBinary(RunViewSchema, view), id, 0n);
try {
  assert.equal(state.terminal, false);
  watchRunAdvance(state, create(RunEventSchema, {
    sequence: 0n, event: { case: "progress", value: { kind: "queued" } },
  }));
  assert.throws(() => watchRunAdvance(state, create(RunEventSchema, {
    sequence: 2n, event: { case: "terminal", value: RunTerminal.COMPLETED },
  })), error => error.code === "invalid");
  assert.equal(state.terminal, false);
  watchRunAdvance(state, create(RunEventSchema, {
    sequence: 1n, event: { case: "terminal", value: RunTerminal.COMPLETED },
  }));
  assert.equal(state.terminal, true);
  watchRunFinish(state);
  assert.throws(() => watchRunAdvance(state, create(RunEventSchema, {
    sequence: 2n, event: { case: "progress", value: { kind: "late" } },
  })), error => error.code === "invalid");
} finally {
  state.free();
}
console.log(JSON.stringify({
  status: "passed", publicPathValidation: "preserved",
  compiledRustModule: "ordered-progress-terminal-and-rejected-gap-postterminal",
}));
