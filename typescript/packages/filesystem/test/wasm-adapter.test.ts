import { describe, expect, test } from "bun:test";

import type {
  WasmRawWorkspaceContextRegistry,
} from "../src/contracts.js";
import {
  adaptWasmWorkspaceContextRegistry,
} from "../src/wasm-adapter.js";
import { openBrowserOperationWindowCoordinator } from "../src/browser.js";

const identity = (byte: number): number[] => Array.from({ length: 16 }, () => byte);

describe("WASM adapter canonical boundaries", () => {
  test("preserves u64 context revisions beyond JavaScript's safe integer range", async () => {
    const raw = {
      async resolveJson() {
        return JSON.stringify({
          version: 1,
          revision: "9007199254740993",
          context_id: identity(1),
          parent_context_id: null,
          roots: {},
          state: "active",
        });
      },
    } as unknown as WasmRawWorkspaceContextRegistry;

    const context = await adaptWasmWorkspaceContextRegistry(raw).resolve(Uint8Array.from(identity(1)));
    expect(context.revision).toBe(9007199254740993n);
  });

  test("rejects out-of-range identity bytes instead of wrapping them", async () => {
    const raw = {
      async resolveJson() {
        return JSON.stringify({
          version: 1,
          revision: "1",
          context_id: [-1, ...identity(1).slice(1)],
          parent_context_id: null,
          roots: {},
          state: "active",
        });
      },
    } as unknown as WasmRawWorkspaceContextRegistry;

    await expect(
      adaptWasmWorkspaceContextRegistry(raw).resolve(Uint8Array.from(identity(1))),
    ).rejects.toThrow("context identity must be a 16-byte identity");
  });

  test("rejects unsupported browser operation windows explicitly", async () => {
    await expect(openBrowserOperationWindowCoordinator()).rejects.toThrow(
      "browser operation windows are unsupported by the persistent authority",
    );
  });
});
