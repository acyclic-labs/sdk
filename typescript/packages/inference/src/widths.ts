import { INFERENCE_FIXED_WIDTH_METADATA, type InferenceFixedWidthMetadata } from "../generated/fixed-width-metadata.js";

type WidthEntry = InferenceFixedWidthMetadata;

/** Validate the Rust/reflection metadata before exposing any client widths. */
export function validateInferenceFixedWidthMetadata(raw: readonly WidthEntry[]): readonly WidthEntry[] {
  if (!Array.isArray(raw) || raw.length === 0) throw new Error("inference fixed-width metadata is empty");
  const keys = new Set<string>();
  for (const item of raw) {
    if (item === null || typeof item !== "object" ||
        typeof item.message !== "string" || item.message.length === 0 ||
        typeof item.field !== "string" || item.field.length === 0 ||
        !Number.isSafeInteger(item.width) || item.width <= 0) {
      throw new Error("inference fixed-width metadata has an invalid or non-positive width");
    }
    const key = `${item.message}.${item.field}`;
    if (keys.has(key)) throw new Error(`inference fixed-width metadata repeats ${key}`);
    keys.add(key);
  }
  return raw;
}

const fixedWidths = new Map<string, number>(
  validateInferenceFixedWidthMetadata(INFERENCE_FIXED_WIDTH_METADATA)
    .map(item => [`${item.message}.${item.field}`, item.width] as const),
);

function fixedWidth(message: string, field: string): number {
  const key = `${message}.${field}`;
  const width = fixedWidths.get(key);
  if (width === undefined) throw new Error(`inference fixed-width metadata omits ${key}`);
  return width;
}

/** Semantic fixed widths used by the ergonomic handles and transport client. */
export const INFERENCE_FIXED_WIDTHS = Object.freeze({
  contextRevision: fixedWidth("inference.customer.v1.ContextView", "revision"),
  mutationRevision: fixedWidth("inference.customer.v1.MutationReceipt", "revision"),
  inspectContextRevision: fixedWidth("inference.customer.v1.InspectContextRequest", "revision"),
  runId: fixedWidth("inference.customer.v1.RunView", "run_id"),
  inspectRunId: fixedWidth("inference.customer.v1.InspectRunRequest", "run_id"),
  watchRunId: fixedWidth("inference.customer.v1.WatchRunRequest", "run_id"),
  cancelRunId: fixedWidth("inference.customer.v1.InspectRunRequest", "run_id"),
  warmCommitment: fixedWidth("inference.customer.v1.WarmView", "commitment"),
  inspectWarmCommitment: fixedWidth("inference.customer.v1.InspectWarmRequest", "commitment"),
  renewWarmCommitment: fixedWidth("inference.customer.v1.RenewWarmRequest", "commitment"),
  releaseWarmCommitment: fixedWidth("inference.customer.v1.ReleaseWarmRequest", "commitment"),
  executionProfile: fixedWidth("inference.customer.v1.ModelCapability", "execution_profile"),
  requestClientInstance: fixedWidth("inference.customer.v1.RequestIdentity", "client_instance"),
  requestId: fixedWidth("inference.customer.v1.RequestIdentity", "request_id"),
  evaluationId: fixedWidth("inference.customer.v1.EvaluationView", "evaluation_id"),
  inspectEvaluationId: fixedWidth("inference.customer.v1.InspectEvaluationRequest", "evaluation_id"),
  evaluationSpecDigest: fixedWidth("inference.customer.v1.EvaluationSpec", "spec_digest"),
});
