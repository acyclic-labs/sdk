// Fuzz the actual terminal observer with captured shipping bytes and arbitrary
// byte-fragment schedules: node scripts/graphcoder-e2e/frames.fuzz.mjs SCHEDULE.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { Terminal, screenText } from "../graphcoder-pty/screen.mjs";
import { TerminalFrames, FailureHistory } from "./frames.mjs";

const corpus = JSON.parse(readFileSync(new URL("./fixtures/terminal-frames.json", import.meta.url), "utf8"));
const bytes = Buffer.concat([Buffer.from(corpus.refused.stdout), Buffer.from(corpus.idle.stdout)]);

export async function fuzz(schedule) {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const history = new FailureHistory();
  let refusal, observations = 0;
  const frames = new TerminalFrames(screen, () => {
    assert.ok(frames.ready());
    observations++;
    refusal ??= history.unexpected(screenText(screen));
  });
  try {
    const writes = [];
    for (let offset = 0, index = 0; offset < bytes.length; index++) {
      const width = schedule.length ? 1 + schedule[index % schedule.length] : bytes.length;
      writes.push(frames.write(bytes.subarray(offset, offset + width)));
      offset += width;
    }
    await Promise.all(writes);
    assert.equal(observations, 2, "each original transaction is observed once, independent of pipe fragmentation");
    assert.match(refusal ?? "", /task [0-9a-f-]{36} failed: invalid request: mandatory con/);
    assert.equal(history.unexpected(screenText(screen)), undefined,
      "negative control: observing only the final idle screen loses the original refusal");
  } finally { frames.dispose(); screen.dispose(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    assert.equal(process.argv.length, 3, "provide one byte-fragment schedule file");
    await fuzz(readFileSync(process.argv[2]));
  } catch (error) {
    process.stderr.write(`step=terminal frame fuzz; recovery=node scripts/graphcoder-e2e/frames.fuzz.mjs SCHEDULE\n${error.stack}\n`);
    process.exitCode = 1;
  }
}
