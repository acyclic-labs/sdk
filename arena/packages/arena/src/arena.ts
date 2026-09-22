/** One race: fork N ways, run a worker per fork, probe, let Jev judge, keep the winner. */
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Fork, Change } from "./acyclic.js";
import type { Forker } from "./forks.js";
import { Jev, choice, score, noul, prob, expected, type ChoiceAnswer, type ScoreAnswer, type NoulAnswer } from "./jev.js";
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

  const state = renderState(opts.task, forker.repo, pre, opts.maxFiles ?? 12, opts.maxDiffChars ?? 1500);
  const labels = forks.map((f) => `fork ${f.label}`);
  const questions: Record<string, ReturnType<typeof choice> | ReturnType<typeof score> | ReturnType<typeof noul>> = {
    winner: choice("Which fork best completes the task and should be promoted?", labels),
  };
  forks.forEach((f) => {
    questions[`safe_${f.label}`] = noul(`Fork ${f.label} is safe to land as-is, without breaking existing behaviour.`);
    questions[`complete_${f.label}`] = score(`How completely does fork ${f.label} accomplish the task?`, ["not at all", "partially", "mostly", "fully"]);
  });
  const verdict = await jev.decide(state, questions);
  const win = verdict.answers.winner as ChoiceAnswer;
  const reports: ForkReport[] = pre.map((r) => ({
    ...r,
    pWin: prob(win, `fork ${r.fork.label}`),
    safe: (verdict.answers[`safe_${r.fork.label}`] as NoulAnswer).noul,
    complete: expected(verdict.answers[`complete_${r.fork.label}`] as ScoreAnswer) / 3,
  }));
  reports.forEach((r) => log.append("decision", {
    state_sha: sha(state), agent: "arena", question: "fork verdict", type: "choice", options: labels,
    probs: labels.map((l) => prob(win, l)), chosen_index: labels.indexOf(win.choice), backend: `jev:${jev.model}`, temperature: 1,
    meta: { fork: r.fork.id, label: r.fork.label, model: r.model, safe: r.safe, complete: r.complete, probe_ok: r.probeOk, kind },
  }));
  const ranked = [...reports].sort((a, b) => b.pWin - a.pWin);
  const winner = ranked[0] ?? null;
  say(`judge: ${ranked.map((r) => `${r.fork.label} ${r.pWin.toFixed(2)}`).join("  ")}  | safe ${reports.map((r) => `${r.fork.label}:${r.safe.toFixed(2)}`).join(" ")}  | $${verdict.usage.cost.toFixed(5)}`);

  let promoted = false;
  const minC = opts.minConfidence ?? 0.6;
  if (winner && opts.promote) {
    const ok = winner.pWin >= minC && (opts.requireSafe === false || winner.safe >= 0.5) && winner.probeOk !== false;
    if (ok) {
      try { const out = forker.promote(winner.fork, winner.changes.map((c) => c.path)); promoted = true; log.note("promote", { fork: winner.fork.id, label: winner.fork.label, model: winner.model, pWin: winner.pWin, output: out }); say(`promoted fork ${winner.fork.label} (${winner.model}): ${out}`); }
      catch (e) { log.note("promote_failed", { fork: winner.fork.id, error: String(e) }); say(`promote FAILED: ${String(e).slice(0, 200)}`); }
    }
    else { log.note("promote_skipped", { fork: winner.fork.id, pWin: winner.pWin, safe: winner.safe, probe_ok: winner.probeOk }); say(`not promoted: winner ${winner.fork.label} pWin ${winner.pWin.toFixed(2)} safe ${winner.safe.toFixed(2)} probe ${winner.probeOk}`); }
  }
  for (const r of reports) { forker.drop(r.fork); log.note("fork_drop", { fork: r.fork.id, label: r.fork.label, promoted: promoted && r === winner }); }

  const workerCost = workers.reduce((s, w) => s + w.cost, 0);
  const ms = Date.now() - t0;
  log.append("step", { step: log.kind("step").length, kind, task: opts.task, models: opts.models, n_questions: Object.keys(questions).length, forwards: 1,
    judge_usd: verdict.usage.cost, worker_usd: workerCost, usd: verdict.usage.cost + workerCost, wall_ms: ms,
    winner: winner ? { label: winner.fork.label, model: winner.model, pWin: winner.pWin, safe: winner.safe, probe_ok: winner.probeOk } : null, promoted });
  return { task: opts.task, kind, forks: reports, winner, promoted, judgeCost: verdict.usage.cost, workerCost, ms, state };
}

import { createHash } from "node:crypto";
const sha = (s: string) => createHash("sha256").update(s, "utf8").digest("hex");
export { LABELS };
