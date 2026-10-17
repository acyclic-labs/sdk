import { test, expect } from "bun:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import fc from "fast-check";
import { nativeCredentials } from "../src/native-credentials.js";

const driver = fileURLToPath(new URL("./native-credentials-model.ts", import.meta.url));
for (const fault of ["", "any-release", "no-release", "live-abort-set", "no-abandon"]) {
  test(`native shared credential interleavings: ${fault || "correct"}`, () => {
    const result = spawnSync(process.execPath, [driver], { encoding: "utf8", timeout: 2000,
      env: { ...process.env, STREAM_CREDENTIAL_FAULT: fault } });
    expect(result.error).toBeUndefined();
    if (!fault) {
      expect(result.status, result.stdout + result.stderr).toBe(0);
      expect(result.stdout).toContain("native credential schedules 360");
    } else {
      expect(result.status).not.toBe(0);
      expect(result.stderr).toContain(fault === "no-abandon" ? "abandoned credential must be fenced before abort"
        : fault === "no-release" ? "last request leaked credentials" : "healthy request cancelled");
    }
  });
}

test("credential admission equals the live request set after arbitrary duplicate cleanup", async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.tuple(fc.integer({ min: 0, max: 3 }), fc.boolean()), { maxLength: 32 }), async operations => {
    const live = new Map<number, { controller: AbortController; release: () => void }>();
    let calls = 0;
    let expected = 0;
    let fences = 0, expectedFences = 0;
    const credentials = nativeCredentials(() => { calls++; return "fixture"; }, () => { fences++; });
    for (const [id, open] of operations) {
      const previous = live.get(id);
      if (previous && live.size === 1) expectedFences++;
      previous?.controller.abort(); previous?.release(); previous?.release();
      live.delete(id);
      if (open) {
        const controller = new AbortController();
        live.set(id, { controller, release: credentials.retain(controller.signal) });
      }
      if (live.size === 0) await expect(credentials.resolve()).rejects.toBeInstanceOf(DOMException);
      else { expect(await credentials.resolve()).toBe("fixture"); expected++; }
      expect(calls).toBe(expected);
      expect(fences).toBe(expectedFences);
    }
    if (live.size > 0) expectedFences++;
    for (const { controller, release } of live.values()) { controller.abort(); release(); release(); }
    await expect(credentials.resolve()).rejects.toBeInstanceOf(DOMException);
    expect(calls).toBe(expected);
    expect(fences).toBe(expectedFences);
  }), { numRuns: 64 });
});
