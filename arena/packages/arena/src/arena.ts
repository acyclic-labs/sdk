/** One race: fork N ways, run a worker per fork, probe, let Jev judge, keep the winner. */
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Fork, Change } from "./acyclic.js";
import type { Forker } from "./forks.js";
import { Jev, choice, score, noul, prob, expected, type ChoiceAnswer, type ScoreAnswer, type NoulAnswer, type JevResponse } from "./jev.js";
import { Log } from "./log.js";
import { runWorker, type WorkerResult } from "./opencode.js";

export interface RaceOptions {
  task: string;
  models: string[];                 // one worker per entry; repeat a model to race it against itself
  kind?: string;                    // task kind label for the board
  testCommand?: string;             // run in each fork; its tail becomes the probe
  promote?: boolean;                // land the winner (default false: judge only, drop all)
  minConfidence?: number;           // promote only if P(winner) >= this (default 0.6)
  requireSafe?: boolean;            // and only if the winner's "safe" >= 0.5 (default true)
  workerTimeoutMs?: number;
  maxFiles?: number;
  maxDiffChars?: number;
  onEvent?: (line: string) => void;
}

export interface ForkReport {
  fork: Fork; model: string; worker: WorkerResult; changes: Change[]; probe: string; probeOk: boolean | null;
  pWin: number; safe: number; complete: number;
}

export interface RaceResult {
  task: string; kind: string; forks: ForkReport[]; winner: ForkReport | null; promoted: boolean;
  judgeCost: number; workerCost: number; ms: number; state: string;
}

const LABELS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const SAME = "no meaningful difference";

function unifiedDiff(base: string, fork: string, rel: string, maxChars: number): string {
  const p = spawnSync("diff", ["-u", "--label", `a/${rel}`, "--label", `b/${rel}`, join(base, rel), join(fork, rel)], { encoding: "utf8" });
  let out = p.stdout || "";
  if (!out && p.status !== 0 && p.stderr) {
    try { out = "+++ b/" + rel + "\n" + readFileSync(join(fork, rel), "utf8").split("\n").map((l) => "+" + l).join("\n"); } catch { out = ""; }
  }
  return out.length > maxChars ? out.slice(0, maxChars) + `\n... (+${out.length - maxChars} chars)\n` : out;
}

export function renderState(task: string, repo: string, reports: Array<{ fork: Fork; model: string; changes: Change[]; probe: string }>, maxFiles: number, maxDiffChars: number): string {
  const blocks = reports.map(({ fork, model, changes, probe }) => {
    const lines = [`=== fork ${fork.label} (${model})`, `Changed paths: ${changes.length}`];
    changes.slice(0, maxFiles).forEach((c) => lines.push(`  ${c.status} ${c.path}`));
    if (changes.length > maxFiles) lines.push(`  ... ${changes.length - maxFiles} more`);
    changes.slice(0, maxFiles).filter((c) => c.status === "A" || c.status === "M").forEach((c) => lines.push(unifiedDiff(repo, fork.path, c.path, maxDiffChars).trimEnd()));
    if (probe) lines.push("Test output:", probe.trim());
    return lines.join("\n");
  });
  return `Task:\n${task.trim()}\n\n${blocks.join("\n\n")}`;
}

export async function race(forker: Forker, jev: Jev, log: Log, opts: RaceOptions): Promise<RaceResult> {
  const t0 = Date.now();
  const say = opts.onEvent ?? (() => {});
  const kind = opts.kind ?? "unlabelled";
  const n = opts.models.length;
  if (n < 1) throw new Error("need at least one model");
  const forks = forker.fork(n);
  log.note("fork_open", { task: opts.task, kind, forker: forker.name, forks: forks.map((f, i) => ({ ...f, model: opts.models[i] })) });
  say(`forked ${n} way(s) with ${forker.name}: ${forks.map((f, i) => `${f.label}=${opts.models[i]}`).join("  ")}`);

  const workers = await Promise.all(forks.map((f, i) => {
    say(`worker ${f.label} started on ${opts.models[i]}`);
    return runWorker(f.path, opts.models[i]!, opts.task, { timeoutMs: opts.workerTimeoutMs }).then((w) => {
      say(`worker ${f.label} ${w.ok ? "finished" : "FAILED"} in ${(w.ms / 1000).toFixed(1)}s, ${w.steps} steps, $${w.cost.toFixed(4)}${w.error ? " (" + w.error + ")" : ""}`);
      log.note("worker", { fork: f.id, label: f.label, model: opts.models[i], ok: w.ok, ms: w.ms, cost: w.cost, tokens: w.tokens, steps: w.steps, toolCalls: w.toolCalls, error: w.error ?? null });
      return w;
    });
  }));

  const pre = forks.map((f, i) => {
    const changes = forker.diff(f);
    let probe = "", probeOk: boolean | null = null;
    if (opts.testCommand && changes.length === 0) {
      probe = "(no changes in this fork; tests not run)";
      say(`probe ${f.label}: skipped, no changes`);
    } else if (opts.testCommand) {
      const p = spawnSync("sh", ["-c", opts.testCommand], { cwd: f.path, encoding: "utf8", timeout: 300_000 });
      probeOk = p.status === 0;
      probe = ((p.stdout || "") + (p.stderr || "")).trim().split("\n").slice(-12).join("\n");
      say(`probe ${f.label}: ${probeOk ? "pass" : "fail"}`);
    }
    return { fork: f, model: opts.models[i]!, worker: workers[i]!, changes, probe, probeOk };
  });

  // Two guards against a forced choice. (1) The option set includes "no meaningful difference": on identical
  // forks Jev picks it at 1.00 and the tilt toward "fork A" disappears (labelbias experiment, 24/24 in one
  // call). (2) The arena still asks twice with the forks relabelled by position and averages, which cancels
  // any residual preference for earlier names or earlier positions.
  const orders = n < 2 ? [pre] : [pre, [...pre].reverse()];
  const verdicts: Array<{ v: JevResponse; byId: Map<string, string> }> = [];
  let state = "";
  for (const ordered of orders) {
    const byId = new Map(ordered.map((r, i) => [r.fork.id, LABELS[i] ?? String(i)]));
    const relabelled = ordered.map((r) => ({ ...r, fork: { ...r.fork, label: byId.get(r.fork.id)! } }));
    const st = renderState(opts.task, forker.repo, relabelled, opts.maxFiles ?? 12, opts.maxDiffChars ?? 1500);
    if (!state) state = st;
    const labels = relabelled.map((r) => `fork ${r.fork.label}`);
    const questions: Record<string, ReturnType<typeof choice> | ReturnType<typeof score> | ReturnType<typeof noul>> = {
      winner: choice("Which fork best completes the task and should be promoted?", n > 1 ? [...labels, SAME] : labels),
    };
    relabelled.forEach((r) => {
      questions[`safe_${r.fork.label}`] = noul(`Fork ${r.fork.label} is safe to land as-is, without breaking existing behaviour.`);
      questions[`complete_${r.fork.label}`] = score(`How completely does fork ${r.fork.label} accomplish the task?`, ["not at all", "partially", "mostly", "fully"]);
    });
    verdicts.push({ v: await jev.decide(st, questions), byId });
  }
  const avg = (fn: (v: JevResponse, label: string) => number, id: string) => verdicts.reduce((s, x) => s + fn(x.v, x.byId.get(id)!), 0) / verdicts.length;
  const pSame = n > 1 ? verdicts.reduce((s, x) => s + prob(x.v.answers.winner as ChoiceAnswer, SAME), 0) / verdicts.length : 0;
  const reports: ForkReport[] = pre.map((r) => ({
    ...r,
    pWin: avg((v, l) => prob(v.answers.winner as ChoiceAnswer, `fork ${l}`), r.fork.id),
    safe: avg((v, l) => (v.answers[`safe_${l}`] as NoulAnswer).noul, r.fork.id),
    complete: avg((v, l) => expected(v.answers[`complete_${l}`] as ScoreAnswer) / 3, r.fork.id),
  }));
  const judgeUsd = verdicts.reduce((s, x) => s + x.v.usage.cost, 0);
  const questionsCount = 1 + 2 * n;
  const winIdx = (() => { let bi = 0; reports.forEach((r, i) => { if (r.pWin > reports[bi]!.pWin) bi = i; }); return bi; })();
  if (pSame > 0) say(`referee: P(no meaningful difference) = ${pSame.toFixed(2)}`);
  const labels = forks.map((f) => `fork ${f.label}`);
  reports.forEach((r) => log.append("decision", {
    state_sha: sha(state), agent: "arena", question: "fork verdict", type: "choice", options: labels,
    probs: reports.map((x) => x.pWin), chosen_index: winIdx, backend: `jev:${jev.model}`, temperature: 1,
    meta: { fork: r.fork.id, label: r.fork.label, model: r.model, safe: r.safe, complete: r.complete, probe_ok: r.probeOk, kind, debiased: verdicts.length > 1, p_same: pSame },
  }));
  const ranked = [...reports].sort((a, b) => b.pWin - a.pWin);
  // Jev's run-to-run jitter is up to 0.06 on identical input (24 inputs x 5 repeats), so two forks within
  // that band are a tie: prefer the one whose tests passed, then the cheaper worker, rather than a coin flip.
  const TIE = 0.06;
  if (ranked.length > 1 && (ranked[0]!.pWin - ranked[1]!.pWin <= TIE || pSame >= 0.5)) {
    const top = ranked.filter((r) => ranked[0]!.pWin - r.pWin <= TIE);
    top.sort((a, b) => Number(b.probeOk === true) - Number(a.probeOk === true) || a.worker.cost - b.worker.cost);
    ranked.splice(0, top.length, ...top);
  }
  const winner = ranked[0] ?? null;
  say(`judge: ${ranked.map((r) => `${r.fork.label} ${r.pWin.toFixed(2)}`).join("  ")}  | safe ${reports.map((r) => `${r.fork.label}:${r.safe.toFixed(2)}`).join(" ")}  | $${judgeUsd.toFixed(5)}${verdicts.length > 1 ? " (both orders)" : ""}`);

  let promoted = false;
  const minC = opts.minConfidence ?? 0.6;
  if (winner && opts.promote) {
    // tests are the ground truth when they ran: a passing probe overrides the referee's safety doubt,
    // a failing probe overrides its confidence; the referee alone decides only when there is no probe
    const safeEnough = winner.probeOk === true || opts.requireSafe === false || winner.safe >= 0.5;
    // a tie ("no meaningful difference" or forks within the jitter band) has already been broken by tests
    // then cost; a tie-broken winner with passing tests lands even though its own pWin is low
    const tie = pSame >= 0.5 || (ranked.length > 1 && Math.abs(ranked[0]!.pWin - ranked[1]!.pWin) <= 0.06);
    const confident = winner.pWin >= minC || (tie && winner.probeOk === true);
    const ok = confident && winner.probeOk !== false && safeEnough;
    if (ok) {
      try { const out = forker.promote(winner.fork, winner.changes.map((c) => c.path)); promoted = true; log.note("promote", { fork: winner.fork.id, label: winner.fork.label, model: winner.model, pWin: winner.pWin, output: out }); say(`promoted fork ${winner.fork.label} (${winner.model}): ${out}`); }
      catch (e) { log.note("promote_failed", { fork: winner.fork.id, error: String(e) }); say(`promote FAILED: ${String(e).slice(0, 200)}`); }
    }
    else { log.note("promote_skipped", { fork: winner.fork.id, pWin: winner.pWin, safe: winner.safe, probe_ok: winner.probeOk }); say(`not promoted: winner ${winner.fork.label} pWin ${winner.pWin.toFixed(2)} safe ${winner.safe.toFixed(2)} probe ${winner.probeOk}`); }
  }
  for (const r of reports) { forker.drop(r.fork); log.note("fork_drop", { fork: r.fork.id, label: r.fork.label, promoted: promoted && r === winner }); }

  const workerCost = workers.reduce((s, w) => s + w.cost, 0);
  const ms = Date.now() - t0;
  log.append("step", { step: log.kind("step").length, kind, task: opts.task, models: opts.models, n_questions: questionsCount, forwards: verdicts.length,
    judge_usd: judgeUsd, worker_usd: workerCost, usd: judgeUsd + workerCost, wall_ms: ms,
    winner: winner ? { label: winner.fork.label, model: winner.model, pWin: winner.pWin, safe: winner.safe, probe_ok: winner.probeOk } : null, promoted });
  return { task: opts.task, kind, forks: reports, winner, promoted, judgeCost: judgeUsd, workerCost, ms, state };
}

import { createHash } from "node:crypto";
const sha = (s: string) => createHash("sha256").update(s, "utf8").digest("hex");
export { LABELS };
