import initRemoteWasm, {
  initSync as initRemoteWasmSync,
  validate_remote_web_ca_certificate,
  validate_remote_web_credential,
  validate_remote_web_content_length,
  validate_remote_web_endpoint,
  validate_remote_web_grpc_endpoint,
  validate_remote_web_message_limit,
  validate_remote_web_response_chunk,
  validate_remote_web_response_limit,
  validate_workers_invoke_deployment,
  validate_workers_invoke_version,
} from "../generated/wasm/acyclic_remote_web_wasm.js";

let ready: Promise<void> | undefined;
let initialized = false;

/** Loads the Rust remote-web boundary before browser HTTP calls. */
export async function ensureWorkersWasm(): Promise<void> {
  if (initialized) return;
  if (ready === undefined) {
    const attempt = (async () => {
      const url = new URL("../generated/wasm/acyclic_remote_web_wasm_bg.wasm", import.meta.url);
      await initRemoteWasm({ module_or_path: url });
      initialized = true;
    })();
    ready = attempt;
    void attempt.catch(() => { if (ready === attempt) ready = undefined; });
  }
  await ready;
}

function errorMessage(error: unknown): string {
  return typeof error === "string" ? error : error instanceof Error ? error.message : String(error);
}

function assertInitialized(): void {
  if (!initialized) throw new TypeError("Rust remote-web WASM is not initialized; await Workers.fromEnv()");
}

export function validateWorkersEndpoint(endpoint: string): void {
  assertInitialized();
  try { validate_remote_web_endpoint(endpoint); }
  catch (error) { throw new TypeError(errorMessage(error)); }
}

export function validateWorkersGrpcEndpoint(endpoint: string): void {
  assertInitialized();
  try { validate_remote_web_grpc_endpoint(endpoint); }
  catch (error) { throw new TypeError(errorMessage(error)); }
}

export function validateWorkersCredential(token: string): void {
  assertInitialized();
  try { validate_remote_web_credential(token); }
  catch (error) { throw new TypeError(errorMessage(error)); }
}

export function validateWorkersCaCertificate(certificate: string): void {
  assertInitialized();
  try { validate_remote_web_ca_certificate(certificate); }
  catch (error) { throw new RangeError(errorMessage(error)); }
}

export function validateWorkersResponseChunk(observed: number, chunk: number, maximum: number): number {
  assertInitialized();
  try { return Number(validate_remote_web_response_chunk(BigInt(observed), BigInt(chunk), BigInt(maximum))); }
  catch (error) { throw new RangeError(errorMessage(error)); }
}

export function validateWorkersResponseLimit(maximum: number): void {
  assertInitialized();
  if (!Number.isSafeInteger(maximum)) throw new RangeError("maximumResponseBytes must be a safe integer");
  try { validate_remote_web_response_limit(BigInt(maximum)); }
  catch (error) { throw new RangeError(errorMessage(error)); }
}

export function validateWorkersMessageLimit(maximum: number): void {
  assertInitialized();
  if (!Number.isSafeInteger(maximum)) throw new RangeError("maximumMessageBytes must be a safe integer");
  try { validate_remote_web_message_limit(BigInt(maximum)); }
  catch (error) { throw new RangeError(errorMessage(error)); }
}

export function validateWorkersContentLength(contentLength: string, maximum: number): void {
  assertInitialized();
  if (!Number.isSafeInteger(maximum)) throw new RangeError("maximumResponseBytes must be a safe integer");
  try { validate_remote_web_content_length(contentLength, BigInt(maximum)); }
  catch (error) { throw new RangeError(errorMessage(error)); }
}

export function validateWorkersInvokeVersion(versionSha256: Uint8Array, method: string): void {
  assertInitialized();
  try { validate_workers_invoke_version(versionSha256, method); }
  catch (error) { throw new TypeError(errorMessage(error)); }
}

export function validateWorkersInvokeDeployment(alias: string, method: string): void {
  assertInitialized();
  try { validate_workers_invoke_deployment(alias, method); }
  catch (error) { throw new TypeError(errorMessage(error)); }
}

function initializeSync(): boolean {
  const runtime = globalThis as typeof globalThis & {
    process?: { getBuiltinModule?: (name: string) => unknown };
  };
  const getBuiltinModule = runtime.process?.getBuiltinModule;
  if (getBuiltinModule === undefined) return false;
  const filesystem = getBuiltinModule("node:fs") as { readFileSync?: (path: URL) => Uint8Array } | undefined;
  if (filesystem?.readFileSync === undefined) return false;
  try {
    initRemoteWasmSync({ module: filesystem.readFileSync(new URL("../generated/wasm/acyclic_remote_web_wasm_bg.wasm", import.meta.url)) });
    initialized = true;
    return true;
  } catch { return false; }
}

if (!initializeSync()) void ensureWorkersWasm().catch(() => {});
