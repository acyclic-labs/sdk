/** Jev routes a task to a model tier and a fan-out. The policy is a table you can edit;
 *  the board (see board.ts) can replace it with what actually won. */
import { Jev, choice, score, noul, expected, type ChoiceAnswer, type ScoreAnswer, type NoulAnswer } from "./jev.js";

export type Tier = "cheap" | "mid" | "frontier";
export interface Route { tier: Tier; models: string[]; fanOut: number; kind: string; complexity: number; risk: number; reasoning: number; parallel: number; cost: number }
export interface Policy { tiers: Record<Tier, string[]>; escalate: Record<Tier, Tier | null> }

export const KINDS = ["rename", "bug fix", "new feature", "refactor", "add tests", "docs", "config or build"];

export const DEFAULT_POLICY: Policy = {
  tiers: {
    cheap: ["deepseek/deepseek-v4-flash"],
    mid: ["anthropic/claude-haiku-4.5"],
    frontier: ["anthropic/claude-sonnet-5"],
  },
  escalate: { cheap: "mid", mid: "frontier", frontier: null },
};

export const ROUTER_QUESTIONS = {
  complexity: score("How complex is this coding task, from trivial to very hard?", ["trivial", "easy", "moderate", "hard", "very hard"]),
  kind: choice("What kind of change is this task?", KINDS),
  risk: score("How risky is it if the change is done wrong?", ["low", "medium", "high"]),
  reasoning: noul("The task needs multi-step reasoning about how parts of the codebase interact."),
  parallel: noul("Several independent attempts at this task would likely produce meaningfully different solutions."),
};

/** Thresholds are expected level indexes: complexity 0..4, risk 0..2; reasoning and parallel are probabilities.
 *  ARENA_ROUTE=aggressive raises the bars (the first eval showed the default over-escalates easy renames). */
export const THRESHOLDS = process.env.ARENA_ROUTE === "aggressive"
  ? { frontierComplexity: 3.0, frontierReasoning: 0.85, midComplexity: 2.0, midRisk: 1.5 }
  : { frontierComplexity: 2.5, frontierReasoning: 0.6, midComplexity: 1.5, midRisk: 1.2 };

export function pickTier(complexity: number, risk: number, reasoning: number, t = THRESHOLDS): Tier {
  if (complexity >= t.frontierComplexity || reasoning >= t.frontierReasoning) return "frontier";
  if (complexity >= t.midComplexity || risk >= t.midRisk) return "mid";
  return "cheap";
}

export async function route(jev: Jev, task: string, opts: { policy?: Policy; context?: string; maxFanOut?: number } = {}): Promise<Route> {
  const policy = opts.policy ?? DEFAULT_POLICY;
  const state = (opts.context ? `Repository context:\n${opts.context}\n\n` : "") + `Task:\n${task}`;
  const r = await jev.decide(state, ROUTER_QUESTIONS);
  const complexity = expected(r.answers.complexity as ScoreAnswer);
  const risk = expected(r.answers.risk as ScoreAnswer);
  const reasoning = (r.answers.reasoning as NoulAnswer).noul;
  const parallel = (r.answers.parallel as NoulAnswer).noul;
  const kind = (r.answers.kind as ChoiceAnswer).choice;
  const tier = pickTier(complexity, risk, reasoning);
  let fanOut = tier === "frontier" ? 2 : 1;
  if (parallel >= 0.6) fanOut += 1;
  fanOut = Math.min(fanOut, opts.maxFanOut ?? 3);
  return { tier, models: policy.tiers[tier], fanOut, kind, complexity, risk, reasoning, parallel, cost: r.usage.cost };
}
