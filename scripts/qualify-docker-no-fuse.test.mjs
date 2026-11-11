import assert from "node:assert/strict";
import test from "node:test";
import { confinement } from "./qualify-docker-no-fuse.mjs";

const mounts = "1 0 0:1 / / rw - overlay overlay rw\n";

test("no-FUSE qualification rejects mount authority in either kernel capability set", () => {
  const status = (effective, bounding) => `CapEff:\t${effective.toString(16)}\nCapBnd:\t${bounding.toString(16)}\nSeccomp:\t2\n`;
  // Linux exposes 64-bit capability masks. Exercise every bit independently
  // in both sets, including SYS_ADMIN available only to a setuid helper.
  for (let bit = 0n; bit < 64n; bit++) {
    for (const [effective, bounding] of [[1n << bit, 0n], [0n, 1n << bit]]) {
      const check = () => confinement(status(effective, bounding), false, [], mounts);
      if (bit === 21n) assert.throws(check, /CAP_SYS_ADMIN/);
      else assert.doesNotThrow(check);
    }
  }
});

test("no-FUSE qualification refuses available FUSE, helpers and absent security evidence", () => {
  const status = "CapEff:\t0\nCapBnd:\t0\nSeccomp:\t2\n";
  assert.throws(() => confinement(status, true, [], mounts), /\/dev\/fuse/);
  assert.throws(() => confinement(status, false, ["/usr/bin/fusermount3"], mounts), /mount helper/);
  assert.throws(() => confinement(status, false, [], mounts + "2 1 0:2 / /sys/fs/fuse/connections rw - fusectl fusectl rw\n"), /fusectl/);
  for (const missing of [undefined, ""]) {
    assert.throws(() => confinement(status, false, [], missing), /mount namespace evidence/);
  }
  for (const key of ["CapEff", "CapBnd", "Seccomp"]) {
    const missing = status.split("\n").filter(line => !line.startsWith(key)).join("\n");
    assert.throws(() => confinement(missing, false, [], mounts));
  }
  for (const mode of ["0", "1"]) {
    assert.throws(() => confinement(status.replace("Seccomp:\t2", `Seccomp:\t${mode}`), false, [], mounts), /seccomp filter/);
  }
});
