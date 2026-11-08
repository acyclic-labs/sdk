import { spawnSync } from "node:child_process";

const claim = process.argv[2] === "--cancellation" ? "journal-cancellation-progress" : "journal-follow-progress";
for (const [program, ...args] of [
  ["bash", "scripts/build-host-fixture.sh"],
  ["bun", "rust/proofs/journal-follow-progress/oracle.ts", ...process.argv.slice(2)],
]) {
  const result = spawnSync(program, args, { stdio: "inherit", windowsHide: true });
  if (result.error || result.status !== 0 || result.signal) {
    console.error(`STEP_FAILED journal-progress ${program}: ${result.error ?? result.status ?? result.signal}; recovery: node rust/proofs/qualify.mjs ${claim}`);
    process.exitCode = 1;
    break;
  }
}
