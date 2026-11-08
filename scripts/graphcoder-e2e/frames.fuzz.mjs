// Fuzz the actual terminal observer with captured shipping bytes and arbitrary
// byte-fragment schedules: node scripts/graphcoder-e2e/frames.fuzz.mjs SCHEDULE.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { Terminal, screenText, screenSnapshot } from "../graphcoder-pty/screen.mjs";
import { TerminalFrames, FailureHistory, queryFailure } from "./frames.mjs";
import { matches } from "./pty.mjs";
import { OutputWatch } from "./process.mjs";

const corpus = JSON.parse(readFileSync(new URL("./fixtures/terminal-frames.json", import.meta.url), "utf8"));
const bytes = Buffer.concat([Buffer.from(corpus.refused.stdout), Buffer.from(corpus.idle.stdout)]);
const query = JSON.parse(readFileSync(new URL("./fixtures/live-terminal-query-refusal.json", import.meta.url), "utf8"));
const interrupted = JSON.parse(readFileSync(new URL("./fixtures/interrupted-terminal.json", import.meta.url), "utf8"));
// Refine the retained capture with the current native renderer's exact marker.
// This is a source-derived mutation, not a newly captured native journey.
const nativeView = readFileSync(new URL("../../rust/crates/harness/src/bin/graphcoder/view.rs", import.meta.url), "utf8");
const refusalFormat = /fn refusal\([^]*?Some\(format!\("([^"]*)"\)\)/.exec(nativeView)?.[1];
assert.ok(refusalFormat?.endsWith("{reason}"), "retain the native refusal renderer source join");
const refusalPrefix = refusalFormat.slice(0, -"{reason}".length)
  .replace(/\\u\{([0-9a-f]+)\}/gi, (_, scalar) => String.fromCodePoint(parseInt(scalar, 16)));
const usageFormat = /refusal\(format_args!\("([^"]*)"/.exec(nativeView)?.[1];
assert.equal(usageFormat, "Usage query ended: {error}", "retain the native usage failure source join");

function admission(visible, text, kind, reason, schedule) {
  const history = new FailureHistory();
  const wrongKind = kind === "task_failure" ? "query_failure" : "task_failure";
  const prefix = reason.slice(0, 1 + (schedule[0] ?? 0) % reason.length);
  const expected = kind === "action_refusal" ? { contains: [prefix] } : { [kind]: prefix };
  assert.equal(history.unexpected(visible), text);
  const rejected = kind === "action_refusal"
    ? [{ task_failure: prefix }, { query_failure: prefix }, { contains: [""] }, { contains: [reason + " changed"] }]
    : [{ [wrongKind]: prefix }, { [kind]: "" }, { [kind]: reason + " changed" }, { contains: [prefix] }];
  for (const condition of rejected) {
    assert.equal(history.unexpected(visible, condition), text);
    history.accept(visible, condition);
    assert.equal(history.unexpected(visible), text, "unadmitted failures cannot enter handled history");
  }
  assert.equal(history.unexpected(visible, expected), undefined);
  history.accept(visible, expected);
  assert.equal(history.unexpected(visible), undefined, "the exact admitted failure survives subsequent actions");
  // Derived mutations vary identity; retained native capture bytes stay intact.
  const changed = kind === "task_failure"
    ? text.replace(reason, "changed " + reason + Buffer.from(schedule).toString("hex"))
    : text + " changed" + Buffer.from(schedule).toString("hex");
  assert.equal(history.unexpected(visible.replace(text, changed)), changed,
    "another reason is not an accepted replay");
  if (kind === "task_failure") {
    const next = text.replace(/[0-9a-f](?= failed:)/, digit => digit === "0" ? "1" : "0");
    assert.notEqual(next, text);
    assert.equal(history.unexpected(visible.replace(text, next)), next,
      "another task with the same reason is not an accepted replay");
  }
  history.accept("", {});
  assert.equal(history.unexpected(visible), text,
    "a later accepted frame evicts absent failures instead of growing a task history");
}

async function recovery(schedule) {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const history = new FailureHistory(), raw = OutputWatch.of("", []);
  let admitted = false, refusal;
  const frames = new TerminalFrames(screen, () => {
    const { visible, diagnostic } = screenSnapshot(screen, { completeLines: true });
    refusal ??= history.unexpected(diagnostic, admitted ? undefined : interrupted.condition);
    if (!admitted && matches(screen, raw, interrupted.condition, visible, diagnostic)) {
      assert.equal(visible, interrupted.accepted, "admit the original captured status transaction");
      history.accept(diagnostic, interrupted.condition);
      admitted = true;
      assert.ok(!matches(screen, raw, interrupted.condition, visible.replace("RECOVERED_E2E", ""), diagnostic),
        "a diagnostic without the retained partial transcript cannot settle recovery");
      assert.ok(!matches(screen, raw, interrupted.condition, visible,
        diagnostic.replace(interrupted.condition.task_failure, "another failure")),
      "another reason cannot settle recovery");
    }
  });
  try {
    const bytes = Buffer.from(interrupted.stdout), marker = Buffer.from("\x1b[?2026l");
    const end = bytes.lastIndexOf(marker) + marker.length;
    await frames.write(bytes.subarray(0, end));
    for (let offset = end, index = 0; offset < bytes.length; index++) {
      const width = schedule.length ? 1 + schedule[index % schedule.length] : bytes.length;
      // Complete each original pipe fragment so partial diagnostics really are
      // observed; queuing all writes would only exercise their final callback.
      await frames.write(bytes.subarray(offset, offset + width));
      offset += width;
    }
    assert.ok(admitted, "the native recovery transaction was observed before teardown");
    assert.equal(refusal, undefined, "the admitted task survives raw diagnostic fragmentation and reflow");
    const final = screenSnapshot(screen).diagnostic;
    assert.match(final, /model provider unavailable \(connection interrupted\)/);
    assert.equal(history.unexpected(final), undefined);
    admission(final, /task [0-9a-f-]{36} failed: [^\n]*/.exec(final)[0],
      "task_failure", "model provider unavailable (connection interrupted)", schedule);
  } finally { frames.dispose(); screen.dispose(); }
}

export async function fuzz(schedule) {
  await recovery(schedule);
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const history = new FailureHistory();
  let refusal, refusedScreen, observations = 0, available = 0, reused = 0;
  const frames = new TerminalFrames(screen, () => {
    assert.ok(frames.ready());
    if (frames.available()) available++;
    frames.consume();
    if (frames.available()) reused++;
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
    assert.equal(available, 2, "the next original transaction releases input admission");
    assert.equal(reused, 0, "request callbacks cannot reuse consumed cells");
    assert.equal(frames.available(), false);
    // Out-of-band cursor bytes come from the retained capture. They must still
    // wake failure observation without releasing the consumed native frame.
    const cursor = corpus.idle.stdout.match(/\x1b\[\?25[hl]/)[0];
    let outOfBand = 0, released = false;
    frames.frame = () => { outOfBand++; released ||= frames.available(); };
    await frames.write(cursor);
    assert.equal(outOfBand, 1);
    assert.equal(released, false, "out-of-band observation cannot re-admit the consumed transaction");
    assert.match(refusal ?? "", /task [0-9a-f-]{36} failed: invalid request: mandatory con/);
    assert.equal(history.unexpected(screenText(screen)), undefined,
      "negative control: observing only the final idle screen loses the original refusal");
    admission(refusedScreen, refusal, "task_failure", refusal.slice(refusal.indexOf(" failed: ") + 9), schedule);
    const notice = query.visible.slice(query.visible.lastIndexOf("\n") + 1);
    assert.equal(queryFailure(query.visible), notice);
    admission(query.visible, notice, "query_failure", notice, schedule);
    for (const reason of [notice, usageFormat.replace("{error}", notice)]) {
      const prefixed = refusalPrefix + reason;
      const current = query.visible.slice(0, -notice.length) + prefixed;
      assert.equal(queryFailure(current), reason);
      admission(current, prefixed, "query_failure", reason, schedule);
    }
    // An ordinary action refusal still needs explicit refusal admission.
    const reason = refusal.slice(refusal.indexOf(" failed: ") + 9);
    const refused = refusalPrefix + reason;
    admission(query.visible.slice(0, -notice.length) + refused, refused, "action_refusal", reason, schedule);
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
