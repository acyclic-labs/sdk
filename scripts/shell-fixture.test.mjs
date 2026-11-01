import assert from "node:assert/strict";
import { join } from "node:path";
import test from "node:test";
import { runShellFixture } from "./shell-fixture.mjs";

test("original shell gates retain early records and reject a lost producer", async () => {
  const bash = process.platform === "win32"
    ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  const recovery = "node --test scripts/shell-fixture.test.mjs";
  for (const mode of ["ready", "lost", "invalid"]) {
    const execution = runShellFixture(bash, `set -euo pipefail
exec 5>&1
(
  exec 4>&-
  printf 'producer-armed\\n' >&5
  if [[ '${mode}' == lost ]]; then
    echo 'step=exercise original producer loss: deliberate exit; recovery command: ${recovery}' >&2
    exit 17
  fi
  IFS= read -r event <&3 && [[ "$event" == run ]] || {
    echo 'step=consume original shell gate: closed or invalid record; recovery command: ${recovery}' >&2
    exit 98
  }
) &
producer=$!
exec 3>&-
printf 'foreground-armed\\n'
IFS= read -r event <&4 && [[ "$event" == foreground ]] || {
  echo 'step=consume original foreground gate: closed or invalid record; recovery command: ${recovery}' >&2
  exit 99
}
wait "$producer"
`, {
      gates: 2, env: process.env, recovery,
      event(line, release) {
        if (mode === "lost") return;
        if (line === "producer-armed") release(0, mode === "ready" ? "run" : "foreign");
        if (line === "foreground-armed") release(1, "foreground");
      },
    });
    if (mode === "lost") {
      await assert.rejects(execution, error => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /gate 0 producer closed before its event/);
        assert.ok(error.message.includes(`recovery command: ${recovery}`));
        return true;
      });
    } else {
      const result = await execution;
      assert.equal(result.status, mode === "ready" ? 0 : 98, result.stderr);
      if (mode === "invalid") assert.match(result.stderr, /closed or invalid record/);
    }
  }
});
