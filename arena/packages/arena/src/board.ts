/** The leaderboard: from the log, which model won which kind of task, at what cost. */
import { Log } from "./log.js";
import type { Policy, Tier } from "./router.js";

export interface Cell { model: string; kind: string; races: number; wins: number; safeAvg: number; probePass: number; probeRuns: number; cost: number }
export interface Board { cells: Cell[]; kinds: string[]; models: string[]; races: number; totalUsd: number; judgeUsd: number; workerUsd: number }

export function buildBoard(log: Log): Board {
  const cells = new Map<string, Cell>();
  const key = (m: string, k: string) => `${k}\u001f${m}`;
  const get = (m: string, k: string) => { const c = cells.get(key(m, k)) ?? { model: m, kind: k, races: 0, wins: 0, safeAvg: 0, probePass: 0, probeRuns: 0, cost: 0 }; cells.set(key(m, k), c); return c; };
  const workersByFork = new Map<string, { cost: number; model: string }>();
  for (const r of log.kind("note")) if (r.data.event === "worker") workersByFork.set(String(r.data.fork), { cost: Number(r.data.cost ?? 0), model: String(r.data.model) });
  const decisions = log.kind("decision").filter((r) => r.data.question === "fork verdict");
  let races = 0, judgeUsd = 0, workerUsd = 0;
  for (const s of log.kind("step")) { races += 1; judgeUsd += Number(s.data.judge_usd ?? 0); workerUsd += Number(s.data.worker_usd ?? 0); }
  for (const d of decisions) {
    const meta = d.data.meta as Record<string, unknown>;
    const c = get(String(meta.model), String(meta.kind ?? "unlabelled"));
    c.races += 1;
    // the recorded winner only: a tied verdict credits the fork the tie-break chose, not every fork at the max
    const options = d.data.options as string[];
    const chosen = options[Number(d.data.chosen_index)];
    if (chosen === `fork ${String(meta.label)}`) c.wins += 1;
    c.safeAvg += Number(meta.safe ?? 0);
    if (meta.probe_ok !== null && meta.probe_ok !== undefined) { c.probeRuns += 1; if (meta.probe_ok) c.probePass += 1; }
    c.cost += workersByFork.get(String(meta.fork))?.cost ?? 0;
  }
  for (const c of cells.values()) c.safeAvg = c.races ? c.safeAvg / c.races : 0;
  const list = [...cells.values()].sort((a, b) => a.kind.localeCompare(b.kind) || b.wins / Math.max(b.races, 1) - a.wins / Math.max(a.races, 1));
  return { cells: list, kinds: [...new Set(list.map((c) => c.kind))], models: [...new Set(list.map((c) => c.model))], races, totalUsd: judgeUsd + workerUsd, judgeUsd, workerUsd };
}

export function renderBoard(b: Board): string {
  const rows = [["kind", "model", "races", "win rate", "safe avg", "tests", "cost/race"]];
  for (const c of b.cells) rows.push([c.kind, c.model, String(c.races), (c.wins / Math.max(c.races, 1)).toFixed(2), c.safeAvg.toFixed(2), c.probeRuns ? `${c.probePass}/${c.probeRuns}` : "-", `$${(c.cost / Math.max(c.races, 1)).toFixed(4)}`]);
  const w = rows[0]!.map((_, i) => Math.max(...rows.map((r) => r[i]!.length)));
  const line = (r: string[]) => r.map((v, i) => v.padEnd(w[i]!)).join("  ");
  return [line(rows[0]!), w.map((n) => "-".repeat(n)).join("  "), ...rows.slice(1).map(line), "",
    `${b.races} races · judge $${b.judgeUsd.toFixed(4)} · workers $${b.workerUsd.toFixed(4)} · total $${b.totalUsd.toFixed(4)}`].join("\n");
}

/** Turn the board into a routing policy: per kind, the model with the best win rate, ties broken by cost. Falls back to the given policy. */
export function policyFromBoard(b: Board, fallback: Policy, minRaces = 3): { policy: Policy; byKind: Record<string, string> } {
  const byKind: Record<string, string> = {};
  for (const k of b.kinds) {
    const cands = b.cells.filter((c) => c.kind === k && c.races >= minRaces);
    if (!cands.length) continue;
    cands.sort((a, c) => (c.wins / c.races) - (a.wins / a.races) || (a.cost / a.races) - (c.cost / c.races));
    byKind[k] = cands[0]!.model;
  }
  const policy: Policy = { tiers: { ...fallback.tiers }, escalate: { ...fallback.escalate } };
  return { policy, byKind };
}

export function badgeSvg(b: Board): string {
  const best = b.cells.length ? [...b.cells].sort((a, c) => (c.wins / Math.max(c.races, 1)) - (a.wins / Math.max(a.races, 1)))[0]! : null;
  const label = "repo arena";
  const value = best ? `${best.model.split("/").pop()} · ${(100 * best.wins / Math.max(best.races, 1)).toFixed(0)}% wins` : "no races yet";
  const lw = 7 * label.length + 12, vw = 7 * value.length + 12;
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${lw + vw}" height="20" role="img" aria-label="${label}: ${value}"><rect width="${lw}" height="20" fill="#555"/><rect x="${lw}" width="${vw}" height="20" fill="#2f6b45"/><g fill="#fff" font-family="Verdana,DejaVu Sans,sans-serif" font-size="11"><text x="6" y="14">${label}</text><text x="${lw + 6}" y="14">${value}</text></g></svg>`;
}

export const tierOf = (policy: Policy, model: string): Tier | null => (Object.keys(policy.tiers) as Tier[]).find((t) => policy.tiers[t].includes(model)) ?? null;
