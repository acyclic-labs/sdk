import assert from "node:assert/strict";
import { mkdtempSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

// Every service directory is a child of the acceptance host's resource lease.
// Standalone host tests use the OS temporary directory through the same path.
export function hostFixtureDirectory(prefix, root = process.env.ACYCLIC_TEST_FIXTURE_ROOT ?? tmpdir()) {
  assert.match(prefix, /^[a-zA-Z0-9_.-]+-$/, "fixture prefix must be one path component");
  assert.ok(isAbsolute(root), "fixture root must be absolute");
  return mkdtempSync(join(realpathSync(root), prefix));
}
