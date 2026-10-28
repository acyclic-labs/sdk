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

    const snapshot = await view.checkpointSnapshot(1);
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

    await expect(view.checkpointSnapshot(2)).rejects.toThrow();
    expect(candidateBuilds).toBe(1);
    expect(checkoutState).toEqual({ pending: ["unpublished-change"], publications: 0 });
  });

  test("checkpoint credit bounds started work without limiting the number of mounts", async () => {
    for (const credit of [1, 8, 64]) {
      let active = 0;
      let peak = 0;
      const pending: (() => void)[] = [];
      const count = 129;
      const view = new MountedView(Array.from({ length: count }, (_, index) => ({
        mountId: identity(1), volumeId: identity(2), path: index === 0 ? "/" : `/root-${index}`,
        checkout: { async checkpoint() {
          active += 1;
          peak = Math.max(peak, active);
          await new Promise<void>(resolve => pending.push(resolve));
          active -= 1;
          return { generationId: identity(index), work: {} };
        } } as unknown as FsCheckout,
      })));
      const result = view.checkpointSnapshot(credit);
      expect(active).toBe(credit);
      for (let completed = 0; completed < count; completed += 1) {
        const release = pending.shift();
        expect(release).toBeDefined();
        release!();
        // Drain the released checkpoint and its worker continuation, without a timer.
        await Promise.resolve();
        await Promise.resolve();
      }
      const snapshot = await result;
      expect(active).toBe(0);
      expect(peak).toBe(credit);
      expect(snapshot.mounts).toHaveLength(count);
      for (const mount of snapshot.mounts) {
        const index = mount.path === "/" ? 0 : Number(mount.path.slice(6));
        expect(mount.generationId).toEqual(identity(index));
      }
    }
  });

  test("failure drains admitted siblings and starts no later checkpoint", async () => {
    let fail!: (reason: unknown) => void;
    let release!: () => void;
    const started: string[] = [];
    const checkout = (path: string) => ({ async checkpoint() {
      started.push(path);
      if (path === "/failure") await new Promise<void>((_, reject) => { fail = reject; });
      else await new Promise<void>(resolve => { release = resolve; });
      return { generationId: identity(3), work: {} };
    } }) as unknown as FsCheckout;
    const view = new MountedView(["/failure", "/sibling", "/later", "/"].map(path => ({
      mountId: identity(1), volumeId: identity(2), path, checkout: checkout(path),
    })));
    let settled = false;
    const result = view.checkpointSnapshot(2).then(
      () => { settled = true; return "unexpected success"; },
      error => { settled = true; return error; },
    );
    const refused = new Error("original checkpoint failure");
    fail(refused);
    await Promise.resolve();
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(started).toEqual(["/failure", "/sibling"]);
    release();
    expect(await result).toBe(refused);
    expect(started).toEqual(["/failure", "/sibling"]);
  });

  test("invalid credit starts no checkpoint", async () => {
    let started = 0;
    const checkout = { async checkpoint() { started += 1; } } as unknown as FsCheckout;
    const view = new MountedView([{ mountId: identity(1), volumeId: identity(2), path: "/", checkout }]);
    for (const credit of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      await expect(view.checkpointSnapshot(credit)).rejects.toThrow(RangeError);
    }
    expect(started).toBe(0);
  });

  test("routing selects component ancestors and keeps distinct mount identities fenced", async () => {
    const calls: string[] = [];
    const checkout = (label: string) => ({ async createFile(path: string) { calls.push(`${label}:${path}`); } }) as unknown as FsCheckout;
    const view = new MountedView(["/", "/work", "/work/深い"].map((path, index) => ({
      mountId: identity(index), volumeId: identity(2), path, checkout: checkout(path),
    })));
    for (const path of ["/file", "/work/file", "/work/深い/file", "/work/深い", "/worker/file", "/work/深い別/file"]) {
      await view.createFile(path, new Uint8Array());
    }
    expect(calls).toEqual(["/:/file", "/work:/file", "/work/深い:/file", "/work/深い:/", "/:/worker/file", "/work:/深い別/file"]);
    expect(() => view.rename("/work/file", "/work/深い/file", false)).toThrow();
    expect(() => view.hardLink("/work/file", "/work/深い/file")).toThrow();
  });
});
