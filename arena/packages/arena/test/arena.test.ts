import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Log } from "../src/log.js";
import { Jev, choice, score, noul, expected } from "../src/jev.js";
import { pickTier, route, DEFAULT_POLICY } from "../src/router.js";
import { buildBoard, renderBoard, policyFromBoard, badgeSvg } from "../src/board.js";
import { renderState } from "../src/arena.js";

test("log chains, persists, verifies, detects tampering", () => {
  const dir = mkdtempSync(join(tmpdir(), "arena-"));
  const path = join(dir, "log.jsonl");
  const log = new Log(path);
  log.note("a", { x: 1 }); log.append("decision", { probs: [0.2, 0.8] });
  log.verify();
  const again = new Log(path); assert.equal(again.length, 2); again.verify();
  assert.equal(again.all()[1]!.prev, again.all()[0]!.hash);
  const lines = readFileSync(path, "utf8").trim().split("\n");
  const r = JSON.parse(lines[0]!); r.data.x = 2; lines[0] = JSON.stringify(r);
  writeFileSync(path, lines.join("\n") + "\n");
  assert.throws(() => new Log(path).verify(), /hash mismatch/);
});

const fakeFetch = (answers: Record<string, unknown>, cost = 0.00001): typeof fetch => (async () => new Response(JSON.stringify({ model: "typesafe/jev-1.13-x", answers, usage: { input_tokens: 10, output_tokens: 5, cost } }), { status: 200 })) as unknown as typeof fetch;

test("jev client parses answers and tracks spend", async () => {
  const j = new Jev({ apiKey: "k", fetchImpl: fakeFetch({ q: { type: "noul", noul: 0.9 } }) });
  const r = await j.decide("s", { q: noul("x") });
  assert.equal((r.answers.q as { noul: number }).noul, 0.9); assert.equal(j.calls, 1); assert.ok(j.spent > 0);
  assert.deepEqual(choice("q", ["a", "b"]).criteria, { a: "a", b: "b" });
  assert.equal(expected({ type: "score", score: 0, legend: {}, probabilities: { "0": 0.5, "1": 0.5 }, confidence: 0.5 }), 0.5);
});

test("jev client retries 429 then succeeds", async () => {
  let n = 0;
  const f = (async () => { n++; return n === 1 ? new Response("slow down", { status: 429 }) : new Response(JSON.stringify({ model: "m", answers: { q: { type: "noul", noul: 1 } }, usage: { input_tokens: 1, output_tokens: 1, cost: 0 } }), { status: 200 }); }) as unknown as typeof fetch;
  const j = new Jev({ apiKey: "k", fetchImpl: f, retries: 2 });
  const r = await j.decide("s", { q: noul("x") }); assert.equal((r.answers.q as { noul: number }).noul, 1); assert.equal(n, 2);
});

test("router maps Jev answers to tiers and fan-out", async () => {
  assert.equal(pickTier(0.5, 0.2, 0.1), "cheap"); assert.equal(pickTier(1.8, 0.5, 0.2), "mid"); assert.equal(pickTier(1.0, 1.5, 0.2), "mid"); assert.equal(pickTier(3.2, 0.1, 0.1), "frontier"); assert.equal(pickTier(1.0, 0.1, 0.9), "frontier");
  const j = new Jev({ apiKey: "k", fetchImpl: fakeFetch({
    complexity: { type: "score", score: 3.1, legend: {}, probabilities: { "0": 0, "1": 0, "2": 0.2, "3": 0.6, "4": 0.2 }, confidence: 0.6 },
    kind: { type: "choice", choice: "refactor", probabilities: { refactor: 0.8, "bug fix": 0.2 }, confidence: 0.8 },
    risk: { type: "score", score: 1, legend: {}, probabilities: { "0": 0.2, "1": 0.6, "2": 0.2 }, confidence: 0.6 },
    reasoning: { type: "noul", noul: 0.8 }, parallel: { type: "noul", noul: 0.7 } }) });
  const r = await route(j, "split handlers into modules");
  assert.equal(r.tier, "frontier"); assert.equal(r.fanOut, 3); assert.equal(r.kind, "refactor"); assert.deepEqual(r.models, DEFAULT_POLICY.tiers.frontier);
});

test("renderState includes task, diff, and probe", () => {
  const base = mkdtempSync(join(tmpdir(), "base-")); const fork = mkdtempSync(join(tmpdir(), "fork-"));
  writeFileSync(join(base, "a.py"), "def add(a, b):\n    return a - b\n"); writeFileSync(join(fork, "a.py"), "def add(a, b):\n    return a + b\n");
  const s = renderState("fix add", base, [{ fork: { id: "1", path: fork, base: "", label: "A" }, model: "m", changes: [{ status: "M", path: "a.py" }], probe: "1 passed" }], 12, 2000);
  assert.match(s, /^Task:\nfix add/); assert.match(s, /=== fork A \(m\)/); assert.match(s, /-    return a - b/); assert.match(s, /\+    return a \+ b/); assert.match(s, /1 passed/);
});

test("board aggregates wins, safety, tests, cost, and derives routing", () => {
  const log = new Log();
  const add = (kind: string, model: string, label: string, forkId: string, pWin: number[], winnerIdx: number, safe: number, probe: boolean | null, cost: number) => {
    log.note("worker", { fork: forkId, label, model, cost });
    log.append("decision", { question: "fork verdict", options: ["fork A", "fork B"], probs: pWin, chosen_index: winnerIdx, meta: { fork: forkId, label, model, safe, probe_ok: probe, kind } });
  };
  for (let i = 0; i < 3; i++) {
    add("refactor", "cheap/m", "A", `a${i}`, [0.2, 0.8], 1, 0.4, false, 0.01);
    add("refactor", "big/m", "B", `b${i}`, [0.2, 0.8], 1, 0.9, true, 0.1);
    log.append("step", { judge_usd: 0.00003, worker_usd: 0.11 });
  }
  const b = buildBoard(log);
  assert.equal(b.races, 3); assert.equal(b.cells.length, 2);
  const big = b.cells.find((c) => c.model === "big/m")!; assert.equal(big.wins, 3); assert.equal(big.probePass, 3); assert.ok(Math.abs(big.cost - 0.3) < 1e-9);
  assert.match(renderBoard(b), /big\/m/);
  const { byKind } = policyFromBoard(b, DEFAULT_POLICY, 3); assert.equal(byKind.refactor, "big/m");
  assert.match(badgeSvg(b), /<svg/);
});
