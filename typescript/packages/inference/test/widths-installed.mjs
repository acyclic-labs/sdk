// Run in a consumer installed from the assembled package tarball.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const publicUrl = import.meta.resolve("@acyclic-labs/inference");
const root = dirname(dirname(fileURLToPath(publicUrl)));
const { INFERENCE_FIXED_WIDTHS: widths } = await import(pathToFileURL(join(root, "generated/widths.js")).href);
const metadata = await import(pathToFileURL(join(root, "generated/fixed-width-metadata.js")).href);
assert.ok(Object.isFrozen(widths));
assert.equal(Object.keys(widths).length, 17);
assert.throws(() => { widths.runId = 32; }, TypeError);
assert.match(await readFile(join(root, "generated/widths.d.ts"), "utf8"), /readonly "runId": 16;/);
assert.ok(metadata.INFERENCE_FIXED_WIDTH_METADATA.length > 17, "canonical raw metadata remains available");
const { contextRevision, runId, warmCommitment, executionProfile, itemId } = await import(publicUrl);
for (const [construct, width] of [[contextRevision, widths.contextRevision], [runId, widths.runId], [warmCommitment, widths.warmCommitment], [executionProfile, widths.executionProfile]]) {
  assert.equal(construct(new Uint8Array(width)).byteLength, width);
  assert.throws(() => construct(new Uint8Array(width - 1)), TypeError);
  assert.throws(() => construct(new Uint8Array(width + 1)), TypeError);
  assert.throws(() => construct(new Array(width).fill(0)), TypeError);
}
assert.equal(itemId(new Uint8Array(0)).byteLength, 0);
console.log(JSON.stringify({ status: "passed", namedWidths: Object.keys(widths).length, publicConstructors: 4, negativeControls: 13 }));
