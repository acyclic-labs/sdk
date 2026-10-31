import { beforeAll, expect, mock, spyOn, test } from "bun:test";
import { WasmExplorer, WasmStream } from "../generated/wasm/acyclic_harness_wasm.js";
import { AgentExplorer } from "../src/explorer.js";
import { ensureClientWasm } from "../src/wasm-runtime.js";
import { appendRecord, conversation, root, statusPath } from "./actor-records.fixture.js";

beforeAll(ensureClientWasm);

test("suppressed or unavailable animation frames cannot stall reads, live history or cancellation", async () => {
  let first!: () => void, second!: () => void;
  const initial = new Promise<void>(resolve => { first = resolve; });
  const updated = new Promise<void>(resolve => { second = resolve; });
  const previousAnimation = Object.getOwnPropertyDescriptor(globalThis, "requestAnimationFrame");
  const previousCancel = Object.getOwnPropertyDescriptor(globalThis, "cancelAnimationFrame");
  const animation = mock(() => 1), cancel = mock(() => {});
  Object.defineProperties(globalThis, {
    requestAnimationFrame: { configurable: true, value: animation },
    cancelAnimationFrame: { configurable: true, value: cancel },
  });
  const frames = spyOn(WasmExplorer.prototype, "frame");
  const health = spyOn(WasmExplorer.prototype, "health");
  const original = WasmExplorer.prototype.drive;
  const decode = new TextDecoder();
  const drives = spyOn(WasmExplorer.prototype, "drive").mockImplementation(function (this: WasmExplorer) {
    original.call(this);
    const [row] = this.history() as { sequence: bigint; value: Uint8Array | null }[];
    if (row?.value === null || row?.value === undefined) return;
    const value = decode.decode(row.value);
    if (value.includes('"first"')) first();
    if (value.includes('"second"')) second();
  });
  const stream = new WasmStream();
  let explorer: AgentExplorer | undefined;
  try {
    await appendRecord(stream, statusPath, '{"phase":"idle","cursor":0}');
    await appendRecord(stream, `${root}/conversation`, conversation("first", 1n));
    explorer = await AgentExplorer.open(stream, { render() { throw new Error("suppressed paint ran"); } });
    // Opening must drive before it returns, even if the browser never paints.
    expect(drives).toHaveBeenCalled();
    explorer.open(root, 1);
    await initial;
    await appendRecord(stream, `${root}/conversation`, conversation("second", 2n));
    await updated;
    expect(animation).toHaveBeenCalledTimes(1);
    expect(frames).not.toHaveBeenCalled();
    expect(health).not.toHaveBeenCalled();

    const count = drives.mock.calls.length;
    explorer.schedule();
    explorer.schedule();
    explorer.dispose();
    // The queued drive sees closure before it can enter the freed WASM owner.
    await new Promise<void>(resolve => queueMicrotask(resolve));
    expect(drives).toHaveBeenCalledTimes(count);
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(() => explorer?.history()).toThrow();

    // Without an animation API, the same drive microtask also renders.
    // A timer fallback would leave this render pending when open returns.
    Reflect.deleteProperty(globalThis, "requestAnimationFrame");
    const render = mock(() => {});
    explorer = await AgentExplorer.open(stream, { render });
    expect(render).toHaveBeenCalled();
  } finally {
    explorer?.dispose(); stream.free(); drives.mockRestore(); health.mockRestore();
    frames.mockRestore();
    if (previousAnimation) Object.defineProperty(globalThis, "requestAnimationFrame", previousAnimation);
    else Reflect.deleteProperty(globalThis, "requestAnimationFrame");
    if (previousCancel) Object.defineProperty(globalThis, "cancelAnimationFrame", previousCancel);
    else Reflect.deleteProperty(globalThis, "cancelAnimationFrame");
  }
});
