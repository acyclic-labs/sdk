// Fuzz the actual terminal observer with captured shipping bytes and arbitrary
// byte-fragment schedules: node scripts/graphcoder-e2e/frames.fuzz.mjs SCHEDULE.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { Terminal, screenText } from "../graphcoder-pty/screen.mjs";
import { TerminalFrames, FailureHistory, queryFailure } from "./frames.mjs";

const corpus = JSON.parse(readFileSync(new URL("./fixtures/terminal-frames.json", import.meta.url), "utf8"));
const bytes = Buffer.concat([Buffer.from(corpus.refused.stdout), Buffer.from(corpus.idle.stdout)]);
const query = JSON.parse(readFileSync(new URL("./fixtures/live-terminal-query-refusal.json", import.meta.url), "utf8"));

function admission(visible, text, kind, reason, schedule) {
  const history = new FailureHistory();
  const wrongKind = kind === "task_failure" ? "query_failure" : "task_failure";
  const prefix = reason.slice(0, 1 + (schedule[0] ?? 0) % reason.length);
  const expected = kind === "action_refusal" ? { contains: [prefix] } : { [kind]: prefix };
  assert.equal(history.unexpected(visible), text);
  const rejected = kind === "action_refusal"
    ? [{ task_failure: prefix }, { query_failure: prefix }, { contains: [""] }, { contains: [reason + " changed"] }]
    : [{ [wrongKind]: prefix }, { [kind]: "" }, { [kind]: reason + " changed" }];
  for (const condition of rejected) {
    assert.equal(history.unexpected(visible, condition), text);
    history.accept(visible, condition);
    assert.equal(history.unexpected(visible), text, "unadmitted failures cannot enter handled history");
  }
  assert.equal(history.unexpected(visible, expected), undefined);
  history.accept(visible, expected);
  assert.equal(history.unexpected(visible), undefined, "the exact admitted failure survives subsequent actions");
  // Derived mutations vary identity; retained native capture bytes stay intact.
  const changed = text + " changed" + Buffer.from(schedule).toString("hex");
  assert.equal(history.unexpected(visible.replace(text, changed)), changed,
    "another reason is not an accepted replay");
  if (kind === "task_failure") {
    const next = text.replace(/[0-9a-f](?= failed:)/, digit => digit === "0" ? "1" : "0");
    assert.notEqual(next, text);
    assert.equal(history.unexpected(visible.replace(text, next)), next,
      "another task with the same reason is not an accepted replay");
  }
}

export async function fuzz(schedule) {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const history = new FailureHistory();
  let refusal, refusedScreen, observations = 0;
  const frames = new TerminalFrames(screen, () => {
    assert.ok(frames.ready());
    observations++;
    const visible = screenText(screen), failure = history.unexpected(visible);
    if (failure && !refusal) { refusal = failure; refusedScreen = visible; }
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
    admission(refusedScreen, refusal, "task_failure", refusal.slice(refusal.indexOf(" failed: ") + 9), schedule);
    const notice = query.visible.slice(query.visible.lastIndexOf("\n") + 1);
    assert.equal(queryFailure(query.visible), notice);
    admission(query.visible, notice, "query_failure", notice, schedule);
    // Derive the action-status marker from the captured query reason; this
    // exercises admission semantics without claiming another native capture.
    const refused = "\u2717 " + notice;
    admission(query.visible.slice(0, -notice.length) + refused, refused, "action_refusal", notice, schedule);
    assert.equal(queryFailure(notice + "\nordinary status"), undefined,
      "transcript text cannot stand in for the original status row");
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
