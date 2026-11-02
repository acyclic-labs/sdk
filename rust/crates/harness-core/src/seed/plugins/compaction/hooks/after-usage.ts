import type { AfterHook, CompactionConfig, Usage } from "acyclic";

const usd = (value: number | null) =>
  value === null ? "unknown" : `$${value.toFixed(3)}`;

export default (({ delivery, config }) => {
  if (delivery.kind !== "log" || config.notify === null) return [];
  const usage = (delivery.added as Usage[]).at(-1);
  if (!usage || usage.capacity <= 0 || usage.inputTokens / usage.capacity < config.notify) {
    return [];
  }
  const cost = usage.compaction;
  return [{ notice: {
    once: `compaction-${config.notify}`,
    wake: false,
    text: `Context is ${Math.round(100 * usage.inputTokens / usage.capacity)}% full. ` +
      `Summarising now: ${usd(cost.summariseNow)}. ` +
      `Each call without summarising: ${usd(cost.perCallIfNot)}. ` +
      `At ${cost.autoTrimAtTokens} tokens the oldest turns are dropped automatically.`,
  } }];
}) satisfies AfterHook<CompactionConfig>;
