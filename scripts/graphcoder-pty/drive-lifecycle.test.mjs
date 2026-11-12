import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import test from "node:test";
import { main } from "./drive.mjs";

// A real terminal process exercises buffer retirement and abrupt completion
// independently of application shutdown behavior or model responses.
const program = String.raw`
const frame = text => process.stdout.write("\x1b[?2026h\x1b[2J\x1b[H\x1b[1;38;2;17;34;51mGraphCoder " + text + "\x1b[0m\x1b[?2026l");
process.stdin.setRawMode(true);
process.stdin.on("data", bytes => {
  if (bytes.includes(114)) {
    process.stdout.write("\x1b[?1049l\x1b[?1049h");
    frame("AFTER_RESUME");
  }
  if (bytes.includes(113)) process.stdout.write(process.argv[1] === "restore" ? "\x1b[?1049l" : "", () => {
    if (process.argv[1] === "signal") process.kill(process.pid, "SIGTERM");
    else process.exit(process.argv[1] === "exit-code" ? 143 : 0);
  });
});
process.stdout.write("\x1b[?1049h");
frame("BEFORE_RESUME");
`;

for (const mode of ["restore", "active", ...(process.platform === "win32" ? [] : ["signal", "exit-code"])]) {
  const resume = mode === "active";
  const title = {
    restore: "completion retains styled cells through alternate-screen restoration",
    active: "completion retains the active screen after a suspend and resume",
    signal: "completion preserves the executable signal after physical retirement",
    "exit-code": "completion preserves a normal exit code equal to a signal convention",
  }[mode];
  test(title, async () => {
    const name = `screen-${randomUUID()}`;
    let started = false;
    try {
      const first = await main(["start", "--name", name, "--binary", process.execPath,
        "--", "-e", program, mode]);
      started = true;
      if (resume) {
        await main(["keys", name, "text:r"]);
        await main(["wait", name, "--contains", "AFTER_RESUME"]);
      }
      const before = await main(["screen", name, "--raw"]);
      const title = before.cells[0].slice(0, 10);
      assert.equal(title.map(cell => cell.text).join(""), "GraphCoder");
      assert.ok(title.every(cell => cell.bold && cell.fg === 0x112233));
      const completion = main(["wait", name, "--exited"]);
      await main(["keys", name, "text:q"]);
      assert.deepEqual((await completion).exit, mode === "signal"
        ? { code: null, signal: "SIGTERM" }
        : { code: mode === "exit-code" ? 143 : 0, signal: null });
      assert.ok(existsSync(first.directory + "/owner"));
      const after = await main(["screen", name, "--raw"]);
      assert.equal(after.text, before.text);
      assert.deepEqual(after.cursor, before.cursor);
      assert.deepEqual(after.cells, before.cells);
      await main(["wait", name, "--contains", resume ? "AFTER_RESUME" : "BEFORE_RESUME"]);
      await assert.rejects(main(["keys", name, "Enter"]), /terminal exited/);
      await assert.rejects(main(["wait", name, "--settled"]), /terminal exited/);
      await main(["stop", name]);
      started = false;
      assert.ok(!existsSync(first.directory + "/owner"));
    } finally {
      if (started) await main(["stop", name]);
    }
  });
}

test("live resize records the new dimensions before their native redraw", async () => {
  const name = `resize-${randomUUID()}`;
  let started = false;
  const program = String.raw`
    process.stdin.setRawMode(true); process.stdin.resume();
    const paint = () => process.stdout.write('\x1b[?2026h\x1b[2J\x1b[HGraphCoder SIZE:' + process.stdout.columns + 'x' + process.stdout.rows + '\x1b[?2026l');
    process.stdout.on('resize', paint); paint();
  `;
  try {
    await main(["start", "--name", name, "--binary", process.execPath, "--cols", "100", "--rows", "30", "--", "-e", program]);
    started = true;
    const recording = await main(["record", name]);
    await main(["resize", name, "--cols", "52", "--rows", "24"]);
    await main(["wait", name, "--contains", "SIZE:52x24"]);
    const events = readFileSync(recording.recording, "utf8").trimEnd().split("\n").slice(1).map(JSON.parse);
    const resize = events.findIndex(event => event[1] === "r" && event[2] === "52x24");
    const paint = events.findIndex(event => event[1] === "o" && event[2].includes("SIZE:52x24"));
    assert.ok(resize >= 0 && paint > resize, "new-size redraw preceded the recording's resize boundary");
  } finally {
    if (started) await main(["stop", name]);
  }
});