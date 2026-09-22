#!/usr/bin/env node
// Compare arms from their logs: tasks landed (tests passed and promoted), cost, wall time, per-task detail.
import { readFileSync, readdirSync } from "node:fs";
import { join, basename } from "node:path";
const dir = process.argv[2]; if (!dir) { console.error("usage: compare.mjs <results dir>"); process.exit(1); }
const arms = readdirSync(dir).filter((f) => f.endsWith(".jsonl")).map((f) => basename(f, ".jsonl"));
const load = (arm) => readFileSync(join(dir, arm + ".jsonl"), "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l));
const rows = {}; const tasks = new Map();
for (const arm of arms) {
  const recs = load(arm); const steps = recs.filter((r) => r.kind === "step"); const promotes = new Set(recs.filter((r) => r.kind === "note" && r.data.event === "promote").map((r) => r.data.fork));
  let landed = 0, usd = 0, judge = 0, ms = 0;
  const per = [];
  for (const s of steps) {
    const ok = !!s.data.promoted && s.data.winner?.probe_ok === true; landed += ok ? 1 : 0; usd += s.data.usd; judge += s.data.judge_usd; ms += s.data.wall_ms;
    per.push({ task: s.data.task, kind: s.data.kind, ok, usd: s.data.usd, ms: s.data.wall_ms, model: s.data.winner?.model ?? "-", pWin: s.data.winner?.pWin ?? 0 });
    tasks.set(s.data.task, s.data.kind);
  }
  rows[arm] = { n: steps.length, landed, usd, judge, ms, per };
}
const f = (x, d = 4) => x.toFixed(d);
let md = `# Repo Arena eval · ${basename(dir)}\n\n| arm | tasks | landed | cost | judge cost | wall | cost per landed task |\n|---|---|---|---|---|---|---|\n`;
for (const [arm, r] of Object.entries(rows)) md += `| ${arm} | ${r.n} | ${r.landed} | $${f(r.usd)} | $${f(r.judge, 5)} | ${(r.ms / 1000).toFixed(0)} s | ${r.landed ? "$" + f(r.usd / r.landed) : "-"} |\n`;
md += `\n## Per task\n\n| kind | task | ${arms.map((a) => `${a}`).join(" | ")} |\n|---|---|${arms.map(() => "---").join("|")}|\n`;
for (const [task, kind] of tasks) md += `| ${kind} | ${task.slice(0, 70)}${task.length > 70 ? "…" : ""} | ${arms.map((a) => { const p = rows[a].per.find((x) => x.task === task); return p ? `${p.ok ? "✅" : "❌"} $${f(p.usd)} ${p.model.split("/").pop()} ${p.pWin ? "(" + p.pWin.toFixed(2) + ")" : ""}` : "-"; }).join(" | ")} |\n`;
md += `\nlanded = tests passed and the winner was promoted. Costs are OpenRouter's own accounting for workers plus Jev. Rerun with \`evals/run.sh\`.\n`;
console.log(md);
