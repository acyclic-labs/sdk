import { describe, expect, test } from "bun:test";
import { PassThrough } from "node:stream";
import { createNativeProcessOwner, NATIVE_PROCESS_OWNER_CAPABILITY, NATIVE_PROCESS_OWNER_VERSION } from "../src/native-process.js";

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
});
