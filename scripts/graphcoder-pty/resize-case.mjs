// A real shipping TUI reflows without needing a provider request.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { main } from "./drive.mjs";

const [binary, argumentsJson] = process.argv.slice(2);
const name = `resize-${randomUUID()}`;
let started = false;
try {
  const first = await main(["start", "--name", name, "--binary", binary,
    "--cols", "100", "--rows", "30", "--", ...JSON.parse(argumentsJson)]);
  started = true;
  const recording = await main(["record", name]);
  let draft = "RESIZE_BEGIN_" + "reflow🙂".repeat(18) + "_RESIZE_END◆";
  await main(["keys", name, `text:${draft}`]);
  const wide = await main(["wait", name, "--contains", "◆"]);
  const root = wide.text.match(/[a-f0-9]{32}/)?.[0];
  assert.ok(root, "the original root must be visible");
  const sizes = [[52, 24], [120, 40], [100, 30]];
  for (const [index, [cols, rows]] of sizes.entries()) {
    await assert.rejects(main(["resize", name, "--cols", "0", "--rows", String(rows)]), /positive native coordinates/);
    await main(["resize", name, "--cols", String(cols), "--rows", String(rows)]);
    // A new editor glyph fences an application frame after native resize.
    // The emulator cannot manufacture this input echo or its new layout.
    const marker = String.fromCodePoint(0x2460 + index);
    draft += marker;
    await main(["keys", name, `text:${marker}`]);
    await main(["wait", name, "--contains", marker]);
    const screen = await main(["screen", name, "--raw"]);
    assert.equal(screen.cols, cols); assert.equal(screen.rows, rows);
    assert.equal(screen.cells.length, rows);
    assert.ok(screen.cells.every(row => row.length === cols));
    assert.equal(screen.text.match(/[a-f0-9]{32}/)?.[0], root);
    assert.ok(screen.text.replace(/\s/g, "").includes(draft), "resize lost editable Unicode input");
    assert.ok(screen.cursor.x >= 0 && screen.cursor.x < cols && screen.cursor.y >= 0 && screen.cursor.y < rows);
    if (cols === 52) assert.ok(screen.visible.split("\n").filter(line => line.trim()).length > wide.visible.split("\n").filter(line => line.trim()).length,
      "the same draft must occupy more rows after narrowing");
    assert.ok(existsSync(first.directory + "/owner"));
  }
  await main(["keys", name, "Ctrl-U"]);
  await main(["wait", name, "--absent", "◆"]);
  const cast = readFileSync(recording.recording, "utf8").trimEnd().split("\n").map(JSON.parse);
  assert.deepEqual(cast.slice(1).filter(event => event[1] === "r").map(event => event[2]), sizes.map(([cols, rows]) => `${cols}x${rows}`));
  console.log("drive deterministic live resize passed");
} finally {
  if (started) await main(["stop", name]);
}
