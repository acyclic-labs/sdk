import type { SwarmDetail, SwarmLimits } from "../src/index.js";
import response from "./fixtures/detail-restored-limits.json" with { type: "json" };

const detail: Pick<SwarmDetail, "swarm_id" | "limits"> = response;
const budget = detail.limits.budget_usd;
if (budget !== null) {
  // @ts-expect-error Restored swarms may omit the budget; a null-only guard is insufficient.
  budget.toFixed(2);
}
if (budget != null) {
  const formatted: string = budget.toFixed(2);
  void formatted;
}
const unbounded: SwarmLimits = { ...detail.limits, budget_usd: null };
void unbounded;
