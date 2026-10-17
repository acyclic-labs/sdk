import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

for (const broken of [false, true]) {
  test(broken ? "negative control rejects public aliases entering Rust Session I/O" : "every Rust error code round-trips through TLS gRPC and WASM Session I/O", () => {
    const child = spawnSync("bun", [fileURLToPath(new URL("./session-error-driver.ts", import.meta.url))], {
      env: { ...process.env, STREAM_SESSION_FAULT: broken ? "aliases" : "" },
      encoding: "utf8", timeout: 5000, windowsHide: true,
    });
    expect(child.error).toBeUndefined();
    if (broken) {
      expect(child.status).not.toBe(0);
      expect(child.stderr).toContain("JS transport must emit canonical Rust code: not_found");
    } else {
      if (child.status !== 0) throw new Error(child.stderr || child.stdout);
      const result = JSON.parse(child.stdout);
      expect(result.connections).toBe(1);
      expect(result.sessions).toBe(result.codes + 2);
    }
  });
}
