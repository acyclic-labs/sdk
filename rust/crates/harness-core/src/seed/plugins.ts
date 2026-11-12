import type { Plugins } from "acyclic";

export default {
  approvals: { enabled: false },
  question: { enabled: false },
  spawn: { enabled: true },
  feedback: { enabled: true },
} satisfies Plugins;
