// The default agent settings. Override them in .volumes/home/agent.ts by exporting
// a value, which replaces these, or a function of them, which keeps every default
// you do not change (and improved ones, when the harness updates):
//
//   import type { AgentConfig } from "sys/types/config";
//   export default (base: AgentConfig): AgentConfig => ({ ...base, sampling: { temperature: 0.2 } });
//
// The kernel starts the fold with the model this session was started with.
import type { AgentConfig } from "sys/types/config";

export default (base: AgentConfig): AgentConfig => base;
