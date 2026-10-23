import { test, expect } from "bun:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const driver = fileURLToPath(new URL("./native-record-filter-driver.ts", import.meta.url));
for (const fault of ["", "dropped-filter", "consecutive", "dropped-headers"]) {
  test(`native record filtering boundary ${fault || "production"}`, () => {
    const result = spawnSync(process.execPath, [driver], { encoding: "utf8", timeout: 2000,
      env: { ...process.env, STREAM_RECORD_FAULT: fault } });
    expect(result.error).toBeUndefined();
    if (!fault) expect(result.status, result.stdout + result.stderr).toBe(0);
    else {
      expect(result.status).not.toBe(0);
      expect(result.stderr).toContain(fault === "dropped-filter" ? "native follow dropped its requested filter"
        : fault === "dropped-headers" ? "native follow dropped record keys" : "stream response contains an invalid record cursor");
    }
  });
}
