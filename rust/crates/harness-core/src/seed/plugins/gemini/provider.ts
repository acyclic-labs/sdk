import type { Config, ProviderProfile } from "sys/types/config";

// Keep the lab-trained declarations from sys/native/google/ as the base:
// read_file, write_file and replace preserve their trained argument shapes.
// They are ordinary data. Replace base.tools here to change them; a changed
// declaration re-prices the context at the next revision, without an approval.
// Inherit cache, compaction, effort and sampling from the selected model.
// Summary calls remove executable built-ins because function-calling NONE
// alone does not disable Gemini's provider-owned tools.
const configure: Config<ProviderProfile> = (base) => ({
  ...base,
  tools: base.tools,
  summaryHeader: "strip-executable-builtins",
});

export default configure;
