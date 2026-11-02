import type { Config, ProviderProfile } from "sys/types/config";

// Keep the lab-trained declarations from sys/native/openai/ as the base:
// shell and apply_patch retain their trained names, schemas and descriptions.
// They are ordinary data. Replace base.tools here to change them; a changed
// declaration re-prices the context at the next revision, without an approval.
// Inherit the selected protocol's cache, compaction, effort and sampling so
// this one settings file works with both Responses and Chat.
const configure: Config<ProviderProfile> = (base) => ({
  ...base,
  tools: base.tools,
  summaryHeader: "strip-executable-builtins",
});

export default configure;
