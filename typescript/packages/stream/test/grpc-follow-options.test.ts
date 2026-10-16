import { test } from "bun:test";
import { fileURLToPath } from "node:url";

test("raw follow declarations accept deadlines and reject connection metadata", () => {
  const fixture = fileURLToPath(new URL("./grpc-follow-options.types.ts", import.meta.url));
  const compiled = Bun.spawnSync(["bun", "x", "tsc", "--ignoreConfig", "--noEmit", "--strict", "--skipLibCheck",
    "--target", "ES2022", "--module", "NodeNext", "--moduleResolution", "NodeNext", fixture], { stdout: "pipe", stderr: "pipe" });
  if (compiled.exitCode !== 0) throw new Error(new TextDecoder().decode(compiled.stdout) + new TextDecoder().decode(compiled.stderr));
});
