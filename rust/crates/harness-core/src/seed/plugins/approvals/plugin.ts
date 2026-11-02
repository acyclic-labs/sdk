import type { PluginConfigs, PluginManifest } from "acyclic";

export default {
  name: "approvals",
  description: "Ask the selected independent approver about the final request.",
  defaults: { mode: "ask" } satisfies PluginConfigs["approvals"],
} satisfies PluginManifest;
