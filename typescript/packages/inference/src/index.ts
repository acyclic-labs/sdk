/** Portable inference request and response. */
export interface InferenceRequest { readonly model: string; readonly messages: readonly string[] }
export interface InferenceResponse { readonly output: string }
/** Public model provider contract. */
export interface InferenceProvider { complete(request: InferenceRequest): Promise<InferenceResponse> }
/** Deterministic model provider. */
export class DeterministicInference implements InferenceProvider { async complete(request: InferenceRequest): Promise<InferenceResponse> { return { output: request.messages.join("\n") }; } }
