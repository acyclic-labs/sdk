import { describe, expect, test } from "bun:test";
import type { FsCheckout } from "../src/contracts.js";
import { MountedView } from "../src/mounted.js";

const identity = (byte: number) => new Uint8Array(16).fill(byte);

describe("mounted checkpoint snapshots", () => {
  test("a failed mount cannot partially publish or consume another checkout's pending state", async () => {
    const checkoutState = { pending: ["unpublished-change"], publications: 0 };
    let candidateBuilds = 0;
    const candidate = {
      acquisitionWork: {},
      async checkpoint() {
        candidateBuilds += 1;
        return { generationId: new TextEncoder().encode(checkoutState.pending.join("\0")), work: {} };
      },
    } as unknown as FsCheckout;
    const failing = {
      acquisitionWork: {},
      async checkpoint() { throw new Error("candidate failed"); },
    } as unknown as FsCheckout;
    const view = new MountedView([
      { mountId: identity(1), volumeId: identity(2), path: "/", checkout: candidate },
      { mountId: identity(3), volumeId: identity(4), path: "/failed", checkout: failing },
    ]);

    await expect(view.checkpointSnapshot()).rejects.toThrow("candidate failed");
    expect(candidateBuilds).toBe(1);
    expect(checkoutState).toEqual({ pending: ["unpublished-change"], publications: 0 });
  });
});
