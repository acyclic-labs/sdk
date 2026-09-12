import type { ModelAttempt, ModelEvent, ModelProvider, ModelRequest } from "@acyclic-labs/harness";

export interface AiSdkStreamResult { readonly fullStream: AsyncIterable<unknown> }
export type AiSdkStreamText = (request: ModelRequest, providerModel: unknown) => AiSdkStreamResult | Promise<AiSdkStreamResult>;

/** Official bridge boundary: callers retain provider-owned immutable model values. */
export function aiSdkProvider(
  resolveModel: (request: ModelRequest) => unknown,
  streamText: AiSdkStreamText,
  project: (part: unknown) => ModelEvent | undefined,
  reconcile: (attempt: ModelAttempt) => Promise<readonly ModelEvent[] | undefined>,
): ModelProvider {
  return {
    async *generate(request) {
      const result = await streamText(request, resolveModel(request));
      for await (const part of result.fullStream) {
        const event = project(part);
        if (event !== undefined) yield event;
      }
    },
    reconcile,
  };
}
