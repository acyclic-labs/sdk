export default {
  name: "compaction",
  description: "Explain context pressure and cache costs before automatic eviction.",
  defaults: { notify: 0.75, reuse: "ancestors" },
  after: { usage: { path: "live/agents/self/usage.jsonl", from: "now" } },
};
