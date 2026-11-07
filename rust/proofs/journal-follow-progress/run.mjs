import { spawnSync } from "node:child_process";

for (const [program, ...args] of [
  ["bash", "scripts/build-host-fixture.sh"],
  ["bun", "rust/proofs/journal-follow-progress/oracle.ts"],
]) {
  const result = spawnSync(program, args, { stdio: "inherit", windowsHide: true });
  if (result.error || result.status !== 0 || result.signal) {
    console.error(`STEP_FAILED journal-progress ${program}: ${result.error ?? result.status ?? result.signal}; recovery: node rust/proofs/qualify.mjs journal-follow-progress`);
    process.exitCode = 1;
    break;
  }
}
