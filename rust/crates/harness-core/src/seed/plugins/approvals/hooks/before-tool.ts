import type { BeforeHook, PluginConfigs } from "acyclic";
import ask from "../modes/ask.ts";
import model from "../modes/model.ts";
import parent from "../modes/parent.ts";

// The configured mode selects who approves; an unknown mode refuses.
export default ((input) => {
  switch (input.config.mode) {
    case "ask":
      return ask(input);
    case "model":
      return model(input);
    case "parent":
      return parent(input);
    default:
      return { allow: false, reason: `unknown approvals mode ${JSON.stringify(input.config.mode)}` };
  }
}) satisfies BeforeHook<PluginConfigs["approvals"]>;
