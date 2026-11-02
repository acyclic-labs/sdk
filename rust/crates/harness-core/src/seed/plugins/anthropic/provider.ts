import type { Config, ProviderProfile } from "sys/types/config";

// Keep the lab-trained declarations from sys/native/anthropic/ as the base:
// their names, argument keys and descriptions match the tools Claude learned.
// They are ordinary data. Replace base.tools here to change them; a changed
// declaration re-prices the context at the next revision, without an approval.
// Override cache, compaction, effort or sampling alongside tools as needed.
const configure: Config<ProviderProfile> = (base) => ({
  ...base,
  tools: base.tools,
  summaryHeader: "strip-executable-builtins",
});

export default configure;
