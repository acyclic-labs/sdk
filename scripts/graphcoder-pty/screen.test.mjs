import assert from "node:assert/strict";
import test from "node:test";
import { Terminal, screenText, styledRun } from "./screen.mjs";
const write = (terminal, bytes) => new Promise(resolve => terminal.write(bytes, resolve));
const terminal = () => new Terminal({ cols: 80, rows: 24, allowProposedApi: true });

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
