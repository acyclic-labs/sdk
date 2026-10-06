import { describe, expect, test } from "bun:test";
import { PassThrough } from "node:stream";
import {
  createNativeProcessOwner,
  createNativeProcessOwnerAdapter,
  NATIVE_PROCESS_OWNER_CAPABILITY,
  NATIVE_PROCESS_OWNER_VERSION,
  type NativeProcessIo,
} from "../src/native-process.js";

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

  test("adapts bounded native stdio and retires cleanup through one token", async () => {
    let outputPolls = 0;
    let terminationCalls = 0;
    const io: NativeProcessIo = {
      launch: () => ({ token: "token-1", pid: 73 }),
      write: async () => undefined,
      closeStdin: () => undefined,
      pollOutput: (_token, stream) => {
        if (stream === "stdout" && outputPolls++ === 0) return { kind: "data", bytes: new TextEncoder().encode("out") };
        if (stream === "stderr" && outputPolls++ === 1) return { kind: "data", bytes: new TextEncoder().encode("err") };
        return { kind: "eof" };
      },
      pollExit: () => ({ kind: "exited", code: 0 }),
      terminate: () => {
        terminationCalls += 1;
        return { kind: "terminated", pid: 73 };
      },
    };
    const owner = createNativeProcessOwnerAdapter(io);
    const child = owner.spawn("fixture", [], {});
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout?.on("data", chunk => stdout.push(Buffer.from(chunk)));
    child.stderr?.on("data", chunk => stderr.push(Buffer.from(chunk)));
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("native child did not close")), 500);
      child.once("close", () => { clearTimeout(timer); resolve(); });
    });
    expect(Buffer.concat(stdout).toString()).toBe("out");
    expect(Buffer.concat(stderr).toString()).toBe("err");
    await expect(owner.terminate(child)).resolves.toEqual({ kind: "terminated", pid: 73 });
    await expect(owner.terminate(child)).resolves.toEqual({ kind: "terminated", pid: 73 });
    expect(terminationCalls).toBe(1);
  });
});
