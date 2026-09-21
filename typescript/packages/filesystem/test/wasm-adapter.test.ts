import { describe, expect, test } from "bun:test";

import type {
  WasmRawOperationWindowCoordinator,
  WasmRawWorkspaceContextRegistry,
} from "../src/contracts.js";
import {
  adaptWasmOperationWindowCoordinator,
  adaptWasmWorkspaceContextRegistry,
} from "../src/wasm-adapter.js";

const identity = (byte: number): number[] => Array.from({ length: 16 }, () => byte);
const generation = (byte: number): number[] => Array.from({ length: 32 }, () => byte);

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

  test("round-trips u64 lease expiry as a decimal string", async () => {
    let finishedLease: unknown;
    const raw = {
      async beginJson() {
        return JSON.stringify({
          workspaceId: identity(1),
          leaseId: identity(2),
          pinnedParent: generation(3),
          expiresAtMillis: "9007199254740993",
        });
      },
      async finishJson(leaseJson: string) {
        finishedLease = JSON.parse(leaseJson);
        return JSON.stringify({ kind: "already-closed" });
      },
    } as unknown as WasmRawOperationWindowCoordinator;
    const coordinator = adaptWasmOperationWindowCoordinator(raw);
    const lease = await coordinator.begin(
      Uint8Array.from(identity(1)),
      Uint8Array.from(generation(3)),
      "test",
      0n,
      9007199254740993n,
    );
    expect(lease.expiresAtMillis).toBe(9007199254740993n);
    await coordinator.finish(lease, 9007199254740993n);
    expect(finishedLease).toMatchObject({ expiresAtMillis: "9007199254740993" });
  });
});
