import { test, expect } from "bun:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
const driver = fileURLToPath(new URL("./native-children-cancel-driver.ts", import.meta.url));
for (const scenario of ["cancel-before-open", "cancel-renewal", "finish-before-cancel", "terminal-error"]) {
  test(`native discovery cancellation: ${scenario}`, () => {
    const result = spawnSync(process.execPath, [driver], { encoding: "utf8", timeout: 2000,
      env: { ...process.env, STREAM_NATIVE_SCENARIO: scenario, STREAM_NATIVE_FAULT: "" } });
    expect(result.error).toBeUndefined();
    expect(result.status, result.stdout + result.stderr).toBe(0);
  });
}
test("negative control detects discovery credentials without cancellation", () => {
  const result = spawnSync(process.execPath, [driver], { encoding: "utf8", timeout: 2000,
    env: { ...process.env, STREAM_NATIVE_SCENARIO: "cancel-renewal", STREAM_NATIVE_FAULT: "missing-signal" } });
  expect(result.error).toBeUndefined();
  expect(result.status).not.toBe(0);
  expect(result.stderr).toContain("credential renewal must receive discovery's cancellation signal");
});
