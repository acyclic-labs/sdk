import type { PluginManifest } from "acyclic";

export default {
  name: "feedback",
  description: "Feedback is cheap and wanted whenever anything slows, blocks or confuses you, including the harness itself. Use write to append one JSON record to live/agents/self/feedback.jsonl: {version:1, kind:'defect'|'slow'|'unclear'|'limit'|'other', what:'one line', evidence:'command, path, error or step', cost?:{seconds?:number,tokens?:number}, suggestion?:string}. The harness stamps agent, step and time.",
  defaults: { retrospective: true },
  after: { settlement: { path: "live/agents/self/status.json", from: "now" } },
} satisfies PluginManifest;
