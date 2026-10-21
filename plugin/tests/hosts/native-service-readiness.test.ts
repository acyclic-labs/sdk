import assert from "node:assert/strict";
import { mkdtempSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { test } from "bun:test";
import { nativeServiceReadinessProbe } from "./native-service-readiness.ts";

test("readiness rechecks the actual marker after a coalesced staged-name notification", () => {
  const directory = mkdtempSync(join(tmpdir(), "acyclic-ready-"));
  try {
    const marker = join(directory, "service.identity");
    const previous = "acyclic-service-v1\nprevious-instance\noriginal-binary\n";
    let observed = 0;
    const probe = nativeServiceReadinessProbe(marker, previous, () => { observed++; });
    probe("rename", null);
    assert.equal(observed, 0, "an absent marker is not ready");
    writeFileSync(marker, previous);
    probe("change", "service.identity");
    assert.equal(observed, 0, "the previous service marker is not ready");
    const staged = join(directory, "service.identity.current-instance.next");
    writeFileSync(staged, "acyclic-service-v1\ncurrent-instance\noriginal-binary\n");
    renameSync(staged, marker);
    // Bun on Windows may omit the final filename and deliver only this hint.
    probe("rename", "service.identity.current-instance.next");
    assert.equal(observed, 1, "a staged-name hint hid the current service marker");
  } finally { rmSync(directory, {recursive:true, force:true}); }
});
