import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { MemoryObjectsV1, ObjectsV1Error, blobDigest, objectsV1Error } from "@acyclic-labs/objects";
import * as wire from "@acyclic-labs/objects/proto";
import { ObjectsV1Memory, ObjectsV1Request } from "../generated/wasm/acyclic_objects_wasm.js";

let original;
let requestBytes;
async function producer() {
  if (original !== undefined) return;
  await MemoryObjectsV1.create();
  const memory = new ObjectsV1Memory();
  let request;
  try {
    request = memory.open("not-an-objects-route", new Uint8Array());
    await request.next();
    assert.fail("original writer must refuse an unknown route");
  } catch (error) {
    assert.equal(error.code, wire.ErrorCode.INVALID_ARGUMENT);
    assert.ok(error.refusal, "original JavaScript refusal writer");
    original = error;
  } finally {
    request?.close();
    request?.free();
    memory.free();
  }
  requestBytes = toBinary(wire.AvailabilityRequestSchema, create(wire.AvailabilityRequestSchema, {
    digest: await blobDigest(new Uint8Array()),
  }));
}

async function observedFailure(failure) {
  let closed = 0;
  const port = { open() {
    return { next: () => Promise.reject(failure), close: () => { closed++; } };
  } };
  const request = ObjectsV1Request.observe(port, requestBytes);
  let actual;
  try {
    await request.next();
  } catch (error) {
    actual = error;
  } finally {
    request.close();
    request.free();
  }
  assert.ok(actual, "original request producer must fail");
  assert.equal(closed, 1, "original failed request retires exactly once");
  return actual;
}

/** Arbitrary diagnostic bytes through the shipped Rust -> JS -> Rust port.
 * The seed is the original Rust writer. Variants are derived adversarial
 * operands, never records of additional provider failures. */
export async function fuzz(input) {
  await producer();
  const raw = {
    ...original.refusal,
    status: (input[0] ?? 14) % 17,
    message: new TextDecoder().decode(input),
    details: new Uint8Array(input),
    metadata: [["acyclic-original-bin", new TextEncoder().encode(Buffer.from(input).toString("base64"))]],
  };
  const error = await observedFailure({ code: original.code, refusal: raw });
  assert.deepEqual(error.refusal, raw, "original JavaScript diagnostic envelope");
  const projected = objectsV1Error(error);
  assert.ok(projected instanceof ObjectsV1Error);
  assert.equal(projected.refusal, error.refusal, "TypeScript retains the same envelope");

  // Differential admission through the generated protobuf reader and the
  // shipped Rust request, including malformed and future semantic codes.
  const future = fromBinary(wire.ErrorDetailSchema, original.refusal.details);
  future.code = 0x7fffffff;
  for (const bytes of [input, toBinary(wire.ErrorDetailSchema, create(wire.ErrorDetailSchema)), toBinary(wire.ErrorDetailSchema, future)]) {
    let code = original.code;
    try {
      const detail = fromBinary(wire.ErrorDetailSchema, bytes);
      if (detail.code !== wire.ErrorCode.UNSPECIFIED && Object.values(wire.ErrorCode).includes(detail.code)) code = detail.code;
    } catch {}
    const detailed = await observedFailure({ code: original.code, detailBytes: new Uint8Array(bytes) });
    assert.equal(detailed.code, code, "unsupported details preserve the original category");
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const paths = process.argv.slice(2);
  if (paths.length === 0) throw new Error("refusal corpus missing; recovery: node typescript/packages/objects/test/refusal.fuzz.mjs rust/codec-fuzz/corpus/protobuf/acyclic.objects.v1.ErrorDetail");
  for (const path of paths) await fuzz(await readFile(path));
  console.log(`replayed ${paths.length} original diagnostic corpus inputs`);
}
