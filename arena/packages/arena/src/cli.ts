#!/usr/bin/env node
/** arena: race OpenCode workers on your repo, let Jev referee, keep the winner, get a board.
 *
 *   arena doctor
 *   arena route "add retry to fetch()"
 *   arena race "add slugify() to util.py" [--models a,b] [--promote] [--test "pytest -q"] [--kind refactor]
 *   arena run tasks.json [--promote] [--test "..."]          # [{"task":"...","kind":"...","models":[...]}]
 *   arena board [--log arena.jsonl] [--badge badge.svg]
 */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { Acyclic } from "./acyclic.js";
import { pickForker } from "./forks.js";
import { Jev } from "./jev.js";
import { Log } from "./log.js";
import { opencodeAvailable } from "./opencode.js";
import { route, DEFAULT_POLICY } from "./router.js";
import { race } from "./arena.js";
import { buildBoard, renderBoard, badgeSvg, policyFromBoard } from "./board.js";

function args(argv: string[]) {
  const flags: Record<string, string | boolean> = {}; const pos: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (a.startsWith("--")) { const k = a.slice(2); const v = argv[i + 1]; if (v !== undefined && !v.startsWith("--")) { flags[k] = v; i++; } else flags[k] = true; }
    else pos.push(a);
  }
  return { flags, pos };
}
const str = (v: string | boolean | undefined, d?: string) => (typeof v === "string" ? v : d);
const loadEnv = () => { try { for (const l of readFileSync(resolve(".env"), "utf8").split("\n")) { const m = /^([A-Z_]+)=(.*)$/.exec(l.trim()); if (m && !process.env[m[1]!]) process.env[m[1]!] = m[2]!.replace(/^["']|["']$/g, ""); } } catch { /* no .env */ } };

async function main(): Promise<number> {
  loadEnv();
  const { flags, pos } = args(process.argv.slice(2));
  const cmd = pos[0];
  const repo = resolve(str(flags.repo, ".")!);
  const logPath = str(flags.log, "arena.jsonl")!;
  const say = (s: string) => console.log(`[arena] ${s}`);

  if (cmd === "doctor") {
    const oc = opencodeAvailable(); console.log(`opencode: ${oc ?? "NOT FOUND (install from opencode.ai)"}`);
    const ac = new Acyclic(repo); console.log(`acyclic:  ${ac.available() ? "ok" : "not found (optional: forks fall back to git worktrees)"}`);
    try { console.log(`forks:    ${pickForker(repo).name}`); } catch (e) { console.log(`forks:    ${String(e)}`); }
    console.log(`OPENROUTER_API_KEY: ${process.env.OPENROUTER_API_KEY ? "set" : "MISSING"}`);
    try { const j = new Jev(); const r = await j.decide("ping", { ok: { type: "noul", instructions: "This is a test." } }); console.log(`jev:      ok (${r.model}, $${r.usage.cost})`); } catch (e) { console.log(`jev:      FAILED ${String(e).slice(0, 200)}`); }
    return 0;
  }
  if (cmd === "route") {
    const task = pos.slice(1).join(" "); if (!task) throw new Error("arena route \"<task>\"");
    const r = await route(new Jev(), task);
    console.log(JSON.stringify(r, null, 2)); return 0;
  }
  if (cmd === "race" || cmd === "run") {
    const jev = new Jev(); const forker = pickForker(repo, (str(flags.forks, "auto") as "auto" | "acyclic" | "git"), [logPath]); const log = new Log(logPath);
    const tasks: Array<{ task: string; kind?: string; models?: string[]; test?: string }> = cmd === "race"
      ? [{ task: pos.slice(1).join(" "), kind: str(flags.kind), models: str(flags.models)?.split(",") }]
      : (JSON.parse(readFileSync(pos[1]!, "utf8")) as Array<{ task: string; kind?: string; models?: string[]; test?: string }>);
    const forceModels = str(flags.models)?.split(",");
    let total = 0;
    const budget = Number(str(flags.budget, "0"));
    const saveStates = str(flags["save-states"]);
    if (saveStates) { const { mkdirSync } = await import("node:fs"); mkdirSync(saveStates, { recursive: true }); }
    for (const t of tasks) {
      if (!t.task) throw new Error("empty task");
      if (budget > 0 && total >= budget) { say(`budget $${budget} reached after $${total.toFixed(4)}; stopping`); log.note("budget_stop", { budget, spent: total }); break; }
      let models = forceModels ?? t.models, kind = t.kind;
      if (!models) {
        const r = await route(jev, t.task);
        models = Array.from({ length: r.fanOut }, (_, i) => r.models[i % r.models.length]!);
        kind = kind ?? r.kind;
        say(`route: ${r.tier} × ${r.fanOut}  (complexity ${r.complexity.toFixed(1)}, risk ${r.risk.toFixed(1)}, reasoning ${r.reasoning.toFixed(2)}, kind ${r.kind})  $${r.cost.toFixed(5)}`);
        log.note("route", { task: t.task, ...r });
      }
      const res = await race(forker, jev, log, { task: t.task, models, kind, testCommand: t.test ?? str(flags.test), promote: flags.promote === true, workerTimeoutMs: Number(str(flags.timeout, "600")) * 1000, onEvent: say });
      total += res.judgeCost + res.workerCost;
      if (saveStates) {
        const verdict = res.forks.map((f) => `fork ${f.fork.label} (${f.model}): pWin ${f.pWin.toFixed(2)} safe ${f.safe.toFixed(2)} complete ${f.complete.toFixed(2)} tests ${f.probeOk}`).join("\n");
        writeFileSync(`${saveStates}/race-${String(log.kind("step").length).padStart(3, "0")}.txt`, `${res.state}\n\n=== verdict\n${verdict}\n`);
      }
      say(`done in ${(res.ms / 1000).toFixed(1)}s · winner ${res.winner ? `${res.winner.fork.label} (${res.winner.model}) p=${res.winner.pWin.toFixed(2)}` : "none"} · ${res.promoted ? "promoted" : "not promoted"} · $${(res.judgeCost + res.workerCost).toFixed(4)}`);
    }
    log.verify();
    say(`${tasks.length} race(s) · $${total.toFixed(4)} · log ${logPath} (${log.length} records, verified)`);
    return 0;
  }
  if (cmd === "board") {
    const log = new Log(logPath); log.verify();
    const b = buildBoard(log); console.log(renderBoard(b));
    const { byKind } = policyFromBoard(b, DEFAULT_POLICY, Number(str(flags["min-races"], "3")));
    if (Object.keys(byKind).length) { console.log("\nrouting from the board (kinds with enough races):"); for (const [k, m] of Object.entries(byKind)) console.log(`  ${k.padEnd(16)} → ${m}`); }
    const badge = str(flags.badge); if (badge) { writeFileSync(badge, badgeSvg(b)); say(`badge written to ${badge}`); }
    const json = str(flags.json); if (json) { writeFileSync(json, JSON.stringify(b, null, 2)); say(`board written to ${json}`); }
    return 0;
  }
  console.log(`arena — race OpenCode workers on your repo, let Jev referee, keep the winner.

  arena doctor
  arena route "<task>"
  arena race "<task>" [--models a,b,c] [--promote] [--test "<cmd>"] [--kind <kind>] [--timeout <s>] [--log arena.jsonl] [--forks auto|acyclic|git]
  arena run tasks.json [--promote] [--test "<cmd>"] [--models a,b] [--budget <usd>] [--save-states dir]
  arena board [--log arena.jsonl] [--badge badge.svg] [--json board.json] [--min-races 3]

Needs: opencode on PATH and OPENROUTER_API_KEY (env or ./.env). acyclic is optional: with it forks are O(1) mounts and promote is a three-way merge; without it, git worktrees and a patch.`);
  return cmd ? 1 : 0;
}

main().then((c) => process.exit(c), (e) => { console.error(`[arena] ${e instanceof Error ? e.message : String(e)}`); process.exit(1); });
