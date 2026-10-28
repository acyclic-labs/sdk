// Run through the shipping shell tool, whose cwd is the active project view.
// The write tool deliberately targets an agent-private volume instead.
import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
try {
  const [path, base64, ...extra] = process.argv.slice(2);
  assert.ok(path && base64 && !extra.length, "expected a path and base64 bytes");
  writeFileSync(path, Buffer.from(base64, "base64"));
  process.stdout.write(readFileSync(path));
} catch (error) {
  console.error(`step=write project fixture: ${error.message}; recovery=node scripts/graphcoder-e2e/write-project.mjs RELATIVE_PATH BASE64_BYTES`);
  process.exitCode = 1;
}
