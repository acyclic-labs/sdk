import { beforeAll, expect, test } from "bun:test";
import fc from "fast-check";
import initialize from "../generated/wasm/acyclic_harness_wasm.js";
import { PageWindow } from "../src/pagination.js";
import { initializeClientQueries } from "../src/client.js";

beforeAll(initializeClientQueries);

const original = { generation: "cut", items: [{ key: "one", value: "original" }],
  before: "before-one", after: "after-one", hasMoreBefore: false, hasMoreAfter: false };
const payload = new TextEncoder().encode(JSON.stringify(original));

test("page admission snapshots intrinsic view bytes across changing backing stores", () => {
  fc.assert(fc.property(
    fc.integer({ min: payload.length + 1, max: 65536 }),
    maximum => {
      for (const shared of [false, true]) for (const resizable of [false, true]) {
        const window = new PageWindow(1, 1, 4096);
        try {
          const Constructor = shared ? SharedArrayBuffer : ArrayBuffer;
          const offset = shared || resizable ? 1 : 0;
          const buffer = resizable ? Reflect.construct(Constructor, [payload.length + offset, { maxByteLength: maximum }])
            : Reflect.construct(Constructor, [shared ? maximum : payload.length]);
          const bytes = new Uint8Array(buffer, offset, payload.length);
          bytes.set(payload);
          let getterCalls = 0;
          for (const name of ["byteLength", "maxByteLength"]) Object.defineProperty(buffer, name, { get: () => { getterCalls++; return 0; } });
          Object.defineProperty(bytes, "buffer", { get: () => { getterCalls++; return new ArrayBuffer(0); } });
          Object.defineProperty(bytes, "length", { get: () => ++getterCalls === 1 ? payload.length : 16 * 1024 * 1024 });
          Object.defineProperty(bytes, "byteLength", { get: () => { getterCalls++; return 0; } });
          window.reset(bytes);
          bytes.fill(0);
          if (resizable) Reflect.apply(Reflect.get(buffer, shared ? "grow" : "resize"), buffer, [maximum]);
          expect(getterCalls).toBe(0);
          expect(window.items()).toEqual(["original"]);
          expect(window.pageCount()).toBe(1);
        } finally { window.free(); }
      }
    },
  ), { numRuns: 32, includeErrorInReport: true });
});

test("oversize page admission refuses before growing WASM memory or changing its view", async () => {
  const window = new PageWindow(1, 1, 4096);
  try {
    window.reset(payload);
    const { memory } = await initialize();
    const oversized = new Uint8Array(Math.max(16 * 1024 * 1024, memory.buffer.byteLength + 65536));
    const revision = window.revision();
    const items = window.items();
    const before = memory.buffer.byteLength;
    let refusal: unknown;
    try { window.reset(oversized); } catch (error) { refusal = error; }
    expect(refusal).toBeInstanceOf(Error);
    expect((refusal as Error).name).toBe("page_capacity");
    expect(memory.buffer.byteLength).toBe(before);
    expect(window.revision()).toBe(revision);
    expect(window.items()).toBe(items);
    expect(items).toEqual(["original"]);
  } finally { window.free(); }
});
