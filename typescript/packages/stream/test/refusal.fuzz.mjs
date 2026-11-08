import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { create, toBinary } from "@bufbuild/protobuf";
import { producer } from "../../objects/test/refusal.fuzz.mjs";
import { HeadsRequestSchema } from "../generated/proto/stream/v1/stream_pb.js";
import init, { WasmSession, grpcErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";

let ready;
async function initialize() {
  ready ??= (async () => init({ module_or_path: await readFile(new URL("../generated/wasm/acyclic_stream_wasm_bg.wasm", import.meta.url)) }))();
  await ready;
}

/** The shared Auth envelope's original Rust writer supplies the seed. Inputs
 * derive adversarial operands, not additional captured member attributions.
 * The shipped session pool owns every request, callback and terminal result. */
export async function fuzz(input) {
  await initialize();
  const original = await producer();
  const request = toBinary(HeadsRequestSchema, create(HeadsRequestSchema, { limit: 1 }));
  for (const status of [...Array(17).keys(), -1, 0x7fffffff]) {
    const raw = {
      ...original.refusal,
      status,
      message: new TextDecoder().decode(input),
      details: new Uint8Array(input),
      metadata: [["acyclic-original-bin", new TextEncoder().encode(Buffer.from(input).toString("base64"))]],
    };
    let calls = 0;
    const session = new WasmSession(() => {
      calls++;
      return Promise.reject({ code: original.code, refusal: raw, transient: false });
    }, true);
    let actual;
    try {
      await session.heads(request, 0);
    } catch (error) {
      actual = error;
    } finally {
      session.free();
    }
    assert.ok(actual, "the original request must finish with its callback failure");
    assert.equal(calls, 1, "a refusal never authorizes another physical connection");
    const category = grpcErrorCode(status, raw.message);
    assert.equal(actual.code, category, "the native and JavaScript boundaries share one category mapping");
    if (category === "unavailable") {
      assert.deepEqual(actual.refusal, raw, "the session pool retains original unknown diagnostic bytes");
    }
    assert.equal(actual.cause, undefined, "opaque diagnostics do not invent typed attribution");
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const paths = process.argv.slice(2);
  if (paths.length === 0) throw new Error("Stream refusal corpus missing; recovery: node typescript/packages/stream/test/refusal.fuzz.mjs rust/codec-fuzz/corpus/protobuf/acyclic.objects.v1.ErrorDetail");
  for (const path of paths) await fuzz(await readFile(path));
  console.log(`replayed ${paths.length} original diagnostic corpus inputs through the session pool`);
}
