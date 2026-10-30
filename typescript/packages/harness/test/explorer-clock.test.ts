import { beforeAll, expect, spyOn, test } from "bun:test";
import { WasmExplorer, WasmStream } from "../generated/wasm/acyclic_harness_wasm.js";
import type { ExplorerHealth } from "../src/explorer.js";
import { ensureClientWasm } from "../src/wasm-runtime.js";

beforeAll(ensureClientWasm);

test.each([-86_400_000, 86_400_000])("wall clock adjustment %i cannot move the reader retry", adjustment => {
  const origin = 1_000;
  let elapsed = origin, wall = 1_800_000_000_000;
  const monotonic = spyOn(performance, "now").mockImplementation(() => elapsed);
  const calendar = spyOn(Date, "now").mockImplementation(() => wall);
  const stream = new WasmStream();
  const core = new WasmExplorer(stream, () => {}, 0);
  try {
    core.networkChanged(false);
    core.drive();
    const initial = core.health() as ExplorerHealth;
    const retry = initial.retryAt;
    if (retry === null) throw new Error("offline reader did not retain its retry deadline");
    expect(initial.connectivity).toBe("offline");
    expect(initial.wakeIn).toBe(retry);
    const advance = Math.floor(Number(retry) / 4);

    wall += adjustment;
    elapsed += advance;
    expect((core.health() as ExplorerHealth).wakeIn).toBe(retry - BigInt(advance));

    elapsed += advance;
    expect((core.health() as ExplorerHealth).wakeIn).toBe(retry - BigInt(advance * 2));

    elapsed = origin + Number(retry);
    expect((core.health() as ExplorerHealth).wakeIn).toBe(0n);
    core.drive();
    expect((core.health() as ExplorerHealth).wakeIn).toBeNull();
    core.networkChanged(true);
    expect((core.health() as ExplorerHealth).connectivity).toBe("online");
  } finally {
    core.free(); stream.free(); calendar.mockRestore(); monotonic.mockRestore();
  }
});
