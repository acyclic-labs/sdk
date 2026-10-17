import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
const driver = fileURLToPath(new URL("./request-capture-driver.ts", import.meta.url));

for (const provider of ["native", "memory", "http", "grpc"]) {
  for (const broken of [false, true]) {
    test(`${provider} captures discovery request before suspension${broken ? " (negative control)" : ""}`, () => {
      const result = spawnSync(process.execPath, [driver], { encoding: "utf8", timeout: 2000,
        env: { ...process.env, STREAM_CAPTURE_PROVIDER: provider, STREAM_CAPTURE_BROKEN: broken ? "1" : "0" } });
      expect(result.error).toBeUndefined();
      if (broken) {
        expect(result.status).not.toBe(0);
        expect(result.stderr).toContain("captured parent");
      } else expect(result.status, result.stdout + result.stderr).toBe(0);
    });
  }
}
