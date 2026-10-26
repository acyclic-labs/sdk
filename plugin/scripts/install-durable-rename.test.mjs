import assert from "node:assert/strict";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { binary, fixture } from "./install-fixture.mjs";

// Publication now enters only through the native package transaction. The old
// generic rename helper could bypass its identity, lock and recovery boundary.
test("bootstrap exposes no arbitrary-path rename entry point in any mode", async () => {
  const owner = fixture();
  try {
    for (const mode of ["test", "production"]) {
      const source = join(owner.root, `${mode} source with spaces`);
      const occupied = join(owner.root, `${mode} occupied destination`);
      const fresh = join(owner.root, `${mode} fresh destination`);
      // Internal namespace inputs, not fabricated output of an external tool.
      writeFileSync(source, "new artifact");
      writeFileSync(occupied, "occupied artifact");
      for (const destination of [occupied, fresh]) {
        for (const publication of ["replace", "no-replace"]) {
          const result = await owner.run(binary,
            ["__installer-rename", source, destination, publication], { NODE_ENV: mode });
          assert.notEqual(result.code, 0, "the retired helper bypassed package publication admission");
          assert.match(result.stderr, /step=admit internal command: unknown __installer-rename/);
          assert.match(result.stderr, /recovery: run acyclic --help/);
          assert.equal(readFileSync(source, "utf8"), "new artifact");
          assert.equal(readFileSync(occupied, "utf8"), "occupied artifact");
          assert.equal(existsSync(fresh), false);
        }
      }
    }
  } finally { await owner.close(); }
});
