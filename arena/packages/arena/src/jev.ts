/** TypeSafe's Jev through OpenRouter's Decisions endpoint (or TypeSafe's own host). */

export type Choice = { type: "choice"; instructions: string; criteria: Record<string, string> };
export type Score = { type: "score"; instructions: string; criteria: string[] };
export type Noul = { type: "noul"; instructions: string };
export type Question = Choice | Score | Noul;

export type ChoiceAnswer = { type: "choice"; choice: string; probabilities: Record<string, number>; confidence: number };
export type ScoreAnswer = { type: "score"; score: number; legend: Record<string, string>; probabilities: Record<string, number>; confidence: number };
export type NoulAnswer = { type: "noul"; noul: number };
export type Answer = ChoiceAnswer | ScoreAnswer | NoulAnswer;

export interface JevResponse {
  model: string;
  answers: Record<string, Answer>;
  usage: { input_tokens: number; output_tokens: number; cost: number };
  id?: string;
  provider?: string;
}

export interface JevOptions {
  apiKey?: string;
  model?: string;
  baseUrl?: string;
  fetchImpl?: typeof fetch;
  retries?: number;
}

export const OPENROUTER_DECISIONS = "https://openrouter.ai/api/alpha/decisions";
export const DEFAULT_MODEL = "typesafe/jev-1.13";

export const choice = (instructions: string, options: string[] | Record<string, string>): Choice => ({
  type: "choice",
  instructions,
  criteria: Array.isArray(options) ? Object.fromEntries(options.map((o) => [o, o])) : options,
});
export const score = (instructions: string, levels: string[]): Score => ({ type: "score", instructions, criteria: levels });
export const noul = (instructions: string): Noul => ({ type: "noul", instructions });

export class Jev {
  readonly model: string;
  readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly fetchImpl: typeof fetch;
  private readonly retries: number;
  spent = 0;
  calls = 0;

  constructor(opts: JevOptions = {}) {
    this.apiKey = opts.apiKey ?? process.env.OPENROUTER_API_KEY ?? process.env.TYPESAFE_API_KEY ?? "";
    if (!this.apiKey) throw new Error("OPENROUTER_API_KEY is not set");
    this.model = opts.model ?? DEFAULT_MODEL;
    this.baseUrl = opts.baseUrl ?? OPENROUTER_DECISIONS;
    this.fetchImpl = opts.fetchImpl ?? fetch;
    this.retries = opts.retries ?? 3;
  }

  async decide(state: string, questions: Record<string, Question>): Promise<JevResponse> {
    const body = JSON.stringify({ model: this.model, state, questions });
    let delay = 1000;
    for (let attempt = 0; ; attempt++) {
      const res = await this.fetchImpl(this.baseUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${this.apiKey}`, "HTTP-Referer": "https://acyclic.dev", "X-Title": "arena" },
        body,
      });
      if (res.ok) {
        const out = (await res.json()) as JevResponse & { error?: unknown };
        if (out.error) throw new Error(`jev: ${JSON.stringify(out.error)}`);
        this.spent += out.usage?.cost ?? 0;
        this.calls += 1;
        return out;
      }
      const text = await res.text();
      if (attempt < this.retries && [429, 500, 502, 503].includes(res.status)) {
        await new Promise((r) => setTimeout(r, delay));
        delay *= 2;
        continue;
      }
      throw new Error(`jev: ${res.status} ${text.slice(0, 400)}`);
    }
  }
}

/** Probability of one option from a choice answer, 0 if absent. */
export const prob = (a: ChoiceAnswer, option: string): number => a.probabilities[option] ?? 0;
/** Expected level index (0-based) from a score answer. */
export const expected = (a: ScoreAnswer): number =>
  Object.entries(a.probabilities).reduce((s, [k, p]) => s + Number(k) * p, 0);
