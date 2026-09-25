import { describe, expect, test } from "bun:test";
import type { FsCheckout } from "../src/contracts.js";
import { MountedView } from "../src/mounted.js";

const identity = (byte: number) => new Uint8Array(16).fill(byte);

describe("mounted checkpoint snapshots", () => {
  test("copies Buffer identities and checkpoint bytes into independent Uint8Arrays", async () => {
    const mountId = Buffer.from(identity(1));
    const volumeId = Buffer.from(identity(2));
    const generationId = Buffer.from(identity(3));
    const checkout = {
      acquisitionWork: {},
      async checkpoint() { return { generationId, work: {} }; },
    } as unknown as FsCheckout;
    const view = new MountedView([{ mountId, volumeId, path: "/", checkout }]);

    const snapshot = await view.checkpointSnapshot();
    const [mount] = snapshot.mounts;
    expect(mount?.mountId).toEqual(identity(1));
    expect(mount?.volumeId).toEqual(identity(2));
    expect(mount?.generationId).toEqual(identity(3));
    expect(Buffer.isBuffer(mount?.mountId)).toBe(false);
    expect(Buffer.isBuffer(mount?.generationId)).toBe(false);
    mount!.mountId[0] = 9;
    mount!.generationId[0] = 9;
    expect(mountId[0]).toBe(1);
    expect(generationId[0]).toBe(3);
  });

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
