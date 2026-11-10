import assert from "node:assert/strict";
import test from "node:test";
import { confinement } from "./qualify-docker-no-fuse.mjs";

test("no-FUSE qualification rejects mount authority in either kernel capability set", () => {
  const status = (effective, bounding) => `CapEff:\t${effective.toString(16)}\nCapBnd:\t${bounding.toString(16)}\nSeccomp:\t2\n`;
  // Linux exposes 64-bit capability masks. Exercise every bit independently
  // in both sets, including SYS_ADMIN available only to a setuid helper.
  for (let bit = 0n; bit < 64n; bit++) {
    for (const [effective, bounding] of [[1n << bit, 0n], [0n, 1n << bit]]) {
      const check = () => confinement(status(effective, bounding), false, []);
      if (bit === 21n) assert.throws(check, /CAP_SYS_ADMIN/);
      else assert.doesNotThrow(check);
    }
  }
});

test("no-FUSE qualification refuses available FUSE, helpers and absent security evidence", () => {
  const status = "CapEff:\t0\nCapBnd:\t0\nSeccomp:\t2\n";
  assert.throws(() => confinement(status, true, []), /\/dev\/fuse/);
  assert.throws(() => confinement(status, false, ["/usr/bin/fusermount3"]), /mount helper/);
  for (const key of ["CapEff", "CapBnd", "Seccomp"]) {
    const missing = status.split("\n").filter(line => !line.startsWith(key)).join("\n");
    assert.throws(() => confinement(missing, false, []));
  }
  for (const mode of ["0", "1"]) {
    assert.throws(() => confinement(status.replace("Seccomp:\t2", `Seccomp:\t${mode}`), false, []), /seccomp filter/);
  }
});
