import { describe, expect, test } from "bun:test";
import { PassThrough } from "node:stream";
import { createNativeProcessOwner, createNativeProcessOwnerAdapter, NATIVE_PROCESS_OWNER_CAPABILITY, NATIVE_PROCESS_OWNER_VERSION } from "../src/native-process.js";

describe("native process owner adapter", () => {
  test("accepts one versioned owner and preserves the Node handle", async () => {
    const child = Object.assign(new PassThrough(), { pid: 41 }) as never;
    let received: unknown;
    const owner = createNativeProcessOwner({
      capability: NATIVE_PROCESS_OWNER_CAPABILITY,
      version: NATIVE_PROCESS_OWNER_VERSION,
      spawn: () => child,
      terminate: async value => {
        received = value;
        return { kind: "terminated", pid: 41 };
      },
    });

    expect(owner.spawn("fixture", [], {})).toBe(child);
    await expect(owner.terminate(child)).resolves.toEqual({ kind: "terminated", pid: 41 });
    expect(received).toBe(child);
  });

  test("rejects missing or incomplete native ownership explicitly", () => {
    expect(() => createNativeProcessOwner(undefined)).toThrow("native companion did not export a process owner");
    expect(() => createNativeProcessOwner({ capability: "other", version: "0.2.0" })).toThrow("owned process capability");
    expect(() => createNativeProcessOwner({ capability: NATIVE_PROCESS_OWNER_CAPABILITY, version: NATIVE_PROCESS_OWNER_VERSION })).toThrow("process owner is incomplete");
  });

  test("reconciles the native registry after a natural root exit", async () => {
    let terminateCalls = 0;
    const io = {
      launch: () => ({ token: "natural-exit-token", pid: 42 }),
      write: () => undefined,
      closeStdin: () => undefined,
      pollOutput: () => ({ kind: "eof" as const }),
      pollExit: () => ({ kind: "exited" as const, code: 0 }),
      terminate: () => {
        terminateCalls += 1;
        return { kind: "terminated" as const, pid: 42 };
      },
    };
    const owner = createNativeProcessOwnerAdapter(io);
    const child = owner.spawn("fixture", [], {});
    await new Promise<void>((resolve, reject) => {
      child.once("close", () => resolve());
      child.once("error", reject);
    });
    expect(terminateCalls).toBe(1);
    await expect(owner.terminate(child)).resolves.toEqual({ kind: "terminated", pid: 42 });
    expect(terminateCalls).toBe(1);
  });

  test("closes after uncertain reader cleanup while retaining the token", async () => {
    const io = {
      launch: () => ({ token: "uncertain-reader-token", pid: 43 }),
      write: () => undefined,
      closeStdin: () => undefined,
      pollOutput: () => ({ kind: "error" as const, reason: "reader disconnected" }),
      pollExit: () => ({ kind: "running" as const }),
      terminate: () => ({ kind: "unknown" as const, pid: 43, reason: "boundary is uncertain" }),
    };
    const owner = createNativeProcessOwnerAdapter(io);
    const child = owner.spawn("fixture", [], {});
    child.on("error", () => undefined);
    await expect(new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("uncertain cleanup did not close")), 1_000);
      child.once("close", () => { clearTimeout(timer); resolve(); });
    })).resolves.toBeUndefined();
    await expect(owner.terminate(child)).resolves.toMatchObject({ kind: "unknown", pid: 43 });
  });

  test("retries a retained launch owner through the typed recovery handle", async () => {
    let terminateCalls = 0;
    const io = {
      launch: () => ({ token: "recovery-token", pid: 44 }),
      write: () => undefined,
      closeStdin: () => undefined,
      pollOutput: () => ({ kind: "idle" as const }),
      pollExit: () => ({ kind: "running" as const }),
      terminate: () => {
        terminateCalls += 1;
        return terminateCalls === 1
          ? { kind: "unknown" as const, pid: 44, reason: "launch initialization failed" }
          : { kind: "terminated" as const, pid: 44 };
      },
    };
    const owner = createNativeProcessOwnerAdapter(io);
    await expect(owner.recoverLaunch({ source: "launch failed", token: "recovery-token", pid: 44 }, 50))
      .resolves.toEqual({ kind: "terminated", pid: 44 });
    expect(terminateCalls).toBe(2);
  });
});
