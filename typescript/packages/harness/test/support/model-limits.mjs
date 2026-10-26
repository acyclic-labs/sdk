/** Finite fixture inputs, not product defaults.
 * @satisfies {import("../../generated/wasm/acyclic_harness_wasm.js").WasmModelLimitsInput} */
export const MODEL_TEST_LIMITS = Object.freeze({
  file_bytes: 65_536, path_bytes: 4_096, attachments: 16, render_bytes: 65_536,
  model_steps: 8, model_events_per_step: 64, tool_calls_per_step: 16, context_messages: 128,
});
