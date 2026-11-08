import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { unlink } from "node:fs/promises";
import { resolve } from "node:path";

test("Node gRPC resumes original follows after physical HTTP/2 loss", async () => {
  const entry = new URL("./session-error-driver.ts", import.meta.url);
  const output = new URL("./session-error-driver.node.mjs", import.meta.url);
  try {
    // Compile only the existing fixture. Node loads the shipped product modules.
    const built = await Bun.build({ entrypoints: [fileURLToPath(entry)], outdir: fileURLToPath(new URL(".", import.meta.url)),
      naming: "session-error-driver.node.mjs",
      target: "node", packages: "external", plugins: [{ name: "shipped-stream", setup(build) {
        build.onResolve({ filter: /(?:^|\/)\.\.\/(?:src|generated)\// }, args => ({
          path: resolve(args.resolveDir, args.path.replace("/src/", "/dist/")), external: true,
        }));
      } }],
    });
    if (!built.success) throw new AggregateError(built.logs, "Node fixture compilation failed");
    const child = spawnSync("node", [fileURLToPath(output)], { encoding: "utf8", windowsHide: true });
    if (child.error) throw child.error;
    if (child.status !== 0) throw new Error(child.stderr || child.stdout);
    expect(JSON.parse(child.stdout).recoveredConnections).toBe(2);
  } finally { await unlink(output).catch(error => { if (error.code !== "ENOENT") throw error; }); }
});

for (const fault of ["", "aliases", "disposition", "rpc-reset"]) {
  test(fault === "aliases" ? "negative control rejects public aliases entering Rust Session I/O"
    : fault === "disposition" ? "negative control detects lost physical reconnect disposition"
    : fault === "rpc-reset" ? "negative control detects an RPC reset on the original live connection"
    : "every Rust error code round-trips through TLS gRPC and WASM Session I/O", () => {
    const child = spawnSync("bun", [fileURLToPath(new URL("./session-error-driver.ts", import.meta.url))], {
      env: { ...process.env, STREAM_SESSION_FAULT: fault },
      encoding: "utf8", windowsHide: true,
    });
    expect(child.error).toBeUndefined();
    if (fault) {
      expect(child.status).not.toBe(0);
      expect(child.stderr).toContain(fault === "aliases" ? "JS transport must emit canonical Rust code: invalid_response"
        : "physical connection loss ended the original follows");
    } else {
      if (child.status !== 0) throw new Error(child.stderr || child.stdout);
      const result = JSON.parse(child.stdout);
      expect(result.connections).toBe(1);
      expect(result.sessions).toBe(result.codes + 1);
      expect(result.recoveredConnections).toBe(2);
    }
  });
}
