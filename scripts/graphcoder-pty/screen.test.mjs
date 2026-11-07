import assert from "node:assert/strict";
import test from "node:test";
import { Terminal, screenText } from "./screen.mjs";
const write = (terminal, bytes) => new Promise(resolve => terminal.write(bytes, resolve));
const terminal = () => new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
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
