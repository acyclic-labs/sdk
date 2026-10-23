import assert from "node:assert/strict";
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { hostFixtureDirectory } from "../../plugin/tests/hosts/fixture-directory.mjs";

test("installed host directories cannot select a parent outside their declared root", () => {
  const root = mkdtempSync(join(tmpdir(), "leased-host-"));
  try {
    const directories = [".pi-native-recursive-", ".pi-native-cuts-", ".pi-native-recursive-"]
      .map(prefix => hostFixtureDirectory(prefix, root));
    assert.equal(new Set(directories).size, 3);
    for (const directory of directories) assert.equal(dirname(directory), realpathSync(root));
    for (const prefix of ["../outside-", "/outside-", "a/b-", "a\\b-"])
      assert.throws(() => hostFixtureDirectory(prefix, root));
    assert.throws(() => hostFixtureDirectory("host-", "relative-root"));
  } finally { rmSync(root, { recursive: true }); }
});
