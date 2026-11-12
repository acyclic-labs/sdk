import assert from "node:assert/strict";
import test from "node:test";
import { Terminal, screenText, styledRun } from "./screen.mjs";
import { screenSnapshot } from "../graphcoder-e2e/screen-text.mjs";
const write = (terminal, bytes) => new Promise(resolve => terminal.write(bytes, resolve));
const terminal = () => new Terminal({ cols: 80, rows: 24, allowProposedApi: true });

test("boxed approval payloads remain observable across interior wraps and transport partitions", async () => {
  const marker = "REPEAT_RULE_BODY";
  for (const prefix of [64, 65, 66, 67, 73, 77]) {
    const payload = "x".repeat(prefix) + marker + " trailing";
    const rows = [];
    for (let at = 0; at < payload.length; at += 78)
      rows.push("│" + payload.slice(at, at + 78).padEnd(78) + "│");
    const bytes = Buffer.from(rows.map((row, y) => `\x1b[${y + 1};1H${row}`).join(""));
    for (const size of [1, 7, bytes.length]) {
      const screen = terminal();
      try {
        for (let at = 0; at < bytes.length; at += size)
          await write(screen, bytes.subarray(at, at + size));
        const snapshot = screenSnapshot(screen);
        assert.equal(snapshot.visible.includes(marker), false, "control must split the payload across borders");
        assert.ok(snapshot.logical.includes(marker), "the actual approval payload disappeared inside its frame");
      } finally { screen.dispose(); }
    }
  }
  const screen = terminal();
  try {
    await write(screen, `│${"first".padEnd(78)}│\x1b[2;1H│${"second".padEnd(78)}│`);
    const { logical, visible } = screenSnapshot(screen);
    assert.ok(logical.includes("first\nsecond"), "short framed rows must retain their line boundary");
    assert.ok(visible.includes("│"), "logical reconstruction must not alter the original visible cells");
  } finally { screen.dispose(); }
});

test("the code oracle rejects unchanged text with missing syntax or background", async () => {
  const screen = terminal();
  try {
    const expected = { text: "fn", fg: [0x61afef, 75, 14], bg: true };
    for (const bytes of ["fn", "\x1b[38;2;97;175;239mfn", "\x1b[48;2;15;16;17mfn", "\x1b[38;2;0;255;0;48;2;15;16;17mfn"]) {
      await write(screen, `\x1b[H\x1b[0m${bytes}\x1b[0m`);
      assert.equal(styledRun(screen, expected), false, "lost styling passed the production oracle");
    }
    await write(screen, "\x1b[H\x1b[38;2;97;175;239;48;2;15;16;17mfn\x1b[0m");
    assert.equal(styledRun(screen, expected), true);
  } finally { screen.dispose(); }
});
test("cursor updates preserve visible admission feedback across transport partitions", async () => {
  const bytes = Buffer.from("\x1b[20;1Hoperation outcome is indeterminate: f0d70643-ac71-4714-936a-6b1481d8650a\x1b[20;1Hconflict:\x1b[20;11Hlocal agent already h\x1b[20;33Hs an a\x1b[20;40Hmitted turn\x1b[K");
  assert.equal(bytes.toString().includes("admitted"), false, "raw substring matching misses this real diff");
  for (const size of [1, 2, 7, bytes.length]) {
    const screen = terminal();
    try {
      for (let at = 0; at < bytes.length; at += size) await write(screen, bytes.subarray(at, at + size));
      assert.ok(screenText(screen).includes("conflict: local agent already has an admitted turn"));
    } finally { screen.dispose(); }
  }
});
