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
const command = JSON.parse(readFileSync(new URL("./fixtures/native-command-refusal.json", import.meta.url), "utf8"));
// Refine the retained capture with the current native renderer's exact marker.
// This is a source-derived mutation, not a newly captured native journey.
const nativeView = readFileSync(new URL("../../plugin/src/runner/view.rs", import.meta.url), "utf8");
const refusalFormat = /fn refusal\([^]*?Some\(format!\("([^"]*)"\)\)/.exec(nativeView)?.[1];
assert.ok(refusalFormat?.endsWith("{reason}"), "retain the native refusal renderer source join");
const refusalPrefix = refusalFormat.slice(0, -"{reason}".length)
  .replace(/\\u\{([0-9a-f]+)\}/gi, (_, scalar) => String.fromCodePoint(parseInt(scalar, 16)));
const queryFormats = [...nativeView.matchAll(/refusal\(format_args!\("([^"]*query ended: \{error\})"/g)]
  .map(match => match[1]);
assert.deepEqual(queryFormats, ["Usage query ended: {error}", "Filesystem query ended: {error}"],
  "retain both native query failure source joins");

// xterm invokes parser observers asynchronously. Return an observer assertion
// through the original write completion so mutation workers can classify it.
function checkedFrames(screen, observe) {
  let failure;
  const frames = new TerminalFrames(screen, () => {
    try { observe(); } catch (error) { failure ??= error; }
  });
  const write = frames.write.bind(frames);
  frames.write = async bytes => {
    await write(bytes);
    if (failure) throw failure;
  };
  return frames;
}

function admission(visible, text, kind, reason, schedule, assertion) {
  const history = new FailureHistory();
  const wrongKind = kind === "task_failure" ? "query_failure" : "task_failure";
  const prefix = reason.slice(0, 1 + (schedule[0] ?? 0) % reason.length);
  const expected = kind === "action_refusal" ? { contains: [prefix] } : { [kind]: prefix };
  assert.equal(history.unexpected(visible), text);
  const rejected = kind === "action_refusal"
    ? [{ task_failure: prefix }, { query_failure: prefix }, { contains: [""] }, { contains: [reason + " changed"] }]
    : [{ [wrongKind]: prefix }, { [kind]: "" }, { [kind]: reason + " changed" }, { contains: [prefix] }];
  for (const condition of rejected) {
    assert.equal(history.unexpected(visible, assertion ? undefined : condition, assertion ? condition : undefined), text);
    history.accept(visible, assertion ? undefined : condition, assertion ? condition : undefined);
    assert.equal(history.unexpected(visible), text, "unadmitted failures cannot enter handled history");
  }
  assert.equal(history.unexpected(visible, assertion ? undefined : expected, assertion ? expected : undefined), undefined);
  history.accept(visible, assertion ? undefined : expected, assertion ? expected : undefined);
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
  let admitted, refusal;
  const frames = checkedFrames(screen, () => {
    const { visible, diagnostic } = screenSnapshot(screen, { completeLines: true });
    refusal ??= history.unexpected(diagnostic, admitted ? undefined : interrupted.condition);
    if (!admitted && matches(screen, raw, interrupted.condition, visible, diagnostic)) {
      history.accept(diagnostic, interrupted.condition);
      admitted = { visible, diagnostic };
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
    assert.equal(admitted.visible, interrupted.accepted, "admit the original captured status transaction");
    assert.ok(!matches(screen, raw, interrupted.condition, admitted.visible.replace("RECOVERED_E2E", ""), admitted.diagnostic),
      "a diagnostic without the retained partial transcript cannot settle recovery");
    assert.ok(!matches(screen, raw, interrupted.condition, admitted.visible,
      admitted.diagnostic.replace(interrupted.condition.task_failure, "another failure")),
    "another reason cannot settle recovery");
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
  const frames = checkedFrames(screen, () => {
    assert.ok(frames.ready());
    observations++;
    if (frames.available()) available++;
    frames.consume();
    if (frames.available()) reused++;
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
    const cursor = corpus.idle.stdout.match(/\x1b\[\?25[hl]/)[0];
    let outOfBand = 0, released = false;
    frames.frame = () => { outOfBand++; released ||= frames.available(); };
    await frames.write(cursor);
    assert.equal(outOfBand, 1);
    assert.equal(released, false, "out-of-band observation cannot re-admit the consumed transaction");
    assert.match(refusal ?? "", /task [0-9a-f-]{36} failed: invalid request: mandatory con/);
    assert.equal(history.unexpected(screenText(screen)), undefined,
      "negative control: observing only the final idle screen loses the original refusal");
    for (const assertion of [false, true]) {
      admission(refusedScreen, refusal, "task_failure", refusal.slice(refusal.indexOf(" failed: ") + 9), schedule, assertion);
    }
    const actionNotice = command.visible.slice(command.visible.lastIndexOf("\n") + 1);
    admission(command.visible, actionNotice, "action_refusal", actionNotice.slice(refusalPrefix.length), schedule);
    const notice = query.visible.slice(query.visible.lastIndexOf("\n") + 1);
    for (const prefix of ["", refusalPrefix]) {
      const current = query.visible.slice(0, -notice.length) + prefix + notice;
      assert.equal(queryFailure(current), undefined);
      admission(current, prefix + notice, "action_refusal", notice, schedule);
    }
    for (const reason of ["resource not found: Unknown command: /marke", ...queryFormats.map(format => format.replace("{error}", notice))]) {
      for (const padding of ["", " ".repeat(1 + (schedule[0] ?? 0) % screen.cols)]) {
        const prefixed = padding + refusalPrefix + reason;
        const current = query.visible.slice(0, -notice.length) + prefixed;
        assert.equal(queryFailure(current), reason);
        admission(current, prefixed, "query_failure", reason, schedule);
      }
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
