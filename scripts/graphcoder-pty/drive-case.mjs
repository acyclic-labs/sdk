// One real-binary deterministic case, invoked by graphcoder-e2e/run.mjs.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { main } from "./drive.mjs";

const [binary, argumentsJson] = process.argv.slice(2);
const name = `fixture-${randomUUID()}`;
let started = false;
try {
  const first = await main(["start", "--name", name, "--binary", binary, "--cols", "100", "--rows", "30",
    "--", ...JSON.parse(argumentsJson)]);
  started = true;
  await main(["wait", name, "--settled"]);
  await main(["keys", name, "Enter"]);
  await main(["wait", name, "--settled"]);
  assert.ok(first.first_paint_ms > 0);
  const record = await main(["record", name]);
  await main(["keys", name, "text:DRIVE_PROMPT"]);
  await main(["wait", name, "--contains", "DRIVE_PROMPT"]);
  const before = await main(["screen", name]);
  assert.ok(before.text.includes("DRIVE_PROMPT"));
  await main(["keys", name, "Enter"]);
  const settled = await main(["wait", name, "--settled"]);
  assert.equal(settled.settled_phase, "idle");
  const answer = await main(["screen", name]);
  assert.ok(answer.text.includes("DRIVE_ANSWER"));
  await main(["wait", name, "--absent", "THIS_TEXT_IS_ABSENT"]);
  const screen = await main(["screen", name, "--raw"]);
  assert.equal(screen.cells.length, 30);
  assert.equal(screen.cells[0].length, 100);
  assert.ok(Number.isInteger(screen.cursor.x));
  const recorded = readFileSync(record.frames, "utf8").trimEnd().split("\n").map(JSON.parse);
  assert.equal(recorded[0].cells.length, 30);
  assert.equal(recorded[0].cells[0].length, 100);
  assert.ok(recorded.slice(1).some(frame => frame.text.includes("DRIVE_ANSWER")));
  for (const frame of recorded.slice(1)) {
    assert.ok(!Object.hasOwn(frame, "cells"), "only the initial frame retains styled cells");
    assert.equal(typeof frame.text, "string");
    assert.ok(Number.isFinite(Date.parse(frame.at)), "logical frames retain timestamps");
  }
  assert.equal(JSON.parse(readFileSync(record.recording, "utf8").split("\n")[0]).version, 2);
  await main(["stop", name]);
  started = false;
  assert.ok(!existsSync(first.directory + "/owner"));
  const restarted = await main(["start", "--name", name, "--binary", binary,
    "--", ...JSON.parse(argumentsJson), "--continue"]);
  started = true;
  assert.notEqual(restarted.artifacts, first.artifacts);
  await main(["wait", name, "--contains", "DRIVE_ANSWER"]);
  await main(["stop", name]);
  started = false;
  console.log("drive deterministic GraphCoder case passed");
} finally {
  if (started) await main(["stop", name]);
}
