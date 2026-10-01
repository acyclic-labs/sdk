import type { CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey, Image, ImageQualification, MachineId, MachineObservation, MachinesProvider, MutationOutcome, OperationId, OperationObservation, SuspensionPolicy, UsageReceipt, MachinePage, MachineEventPage } from "./index.js";
import { httpRoutes, WasmSimulatedMachines } from "../generated/wasm/acyclic_machines_wasm.js";
import type {
  CheckpointOut, EventsOut, MachinesHttpRequest, MachinesHttpResponse, MachinesHttpRoute,
  MachinesHttpRoutes, MutationOut, ObservationOut, OperationOut, PageOut, QualificationOut,
} from "../generated/wasm/acyclic_machines_wasm.js";
import { ensureMachinesWasm } from "./wasm-runtime.js";
import { operationId } from "./index.js";
import { asPublic, usageOut } from "./simulator.js";

export interface HttpMachinesOptions { readonly endpoint: string; readonly token: string; readonly fetcher?: typeof fetch; readonly maximumResponseBytes?: number }

/** Managed-service transport with bounded responses and no implicit mutation retries. */
export class HttpMachinesProvider implements MachinesProvider {
  readonly assurance = "managed-service" as const;
  readonly #endpoint: string; readonly #token: string; readonly #fetcher: typeof fetch; readonly #maximum: number;
  constructor(options: HttpMachinesOptions) { const endpoint = new URL(options.endpoint); if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("endpoint must be an absolute HTTPS URL without credentials, query, or fragment"); if (!options.token.trim()) throw new TypeError("token is required"); const maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024; if (!Number.isSafeInteger(maximum) || maximum <= 0) throw new RangeError("maximumResponseBytes must be a positive safe integer"); this.#endpoint = endpoint.href.endsWith("/") ? endpoint.href : `${endpoint.href}/`; this.#token = options.token; this.#fetcher = options.fetcher ?? fetch; this.#maximum = maximum; }
  qualifyImage(image: Image): Promise<ImageQualification> { return this.#call("IMAGES_QUALIFY", { image }).then(asPublic<QualificationOut, ImageQualification>); }
  create(request: CreateMachine): Promise<MutationOutcome> { return this.#call("MACHINES_CREATE", request).then(asPublic<MutationOut, MutationOutcome>); }
  inspectMachine(machineId: MachineId): Promise<MachineObservation> { return this.#call("MACHINES_INSPECT", { machineId }).then(asPublic<ObservationOut, MachineObservation>); }
  listMachines(after: MachineId | null, limit: number): Promise<MachinePage> { return this.#call("MACHINES_LIST", { after, limit }).then(asPublic<PageOut, MachinePage>); }
  checkpoint(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call("MACHINES_CHECKPOINT", { machineId, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  inspectCheckpoint(checkpointId: CheckpointId): Promise<CheckpointObservation> { return this.#call("CHECKPOINTS_INSPECT", { checkpointId }).then(asPublic<CheckpointOut, CheckpointObservation>); }
  fork(checkpointId: CheckpointId, count: number, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call("CHECKPOINTS_FORK", { checkpointId, count, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  forkMachine(machineId: MachineId, count: number, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call("MACHINES_FORK", { machineId, count, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  suspend(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call("MACHINES_SUSPEND", { machineId, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  wake(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call("MACHINES_WAKE", { machineId, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call("MACHINES_SUSPENSION_POLICY", { machineId, policy, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  destroyMachine(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call("MACHINES_DESTROY", { machineId, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  destroyCheckpoint(checkpointId: CheckpointId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call("CHECKPOINTS_DESTROY", { checkpointId, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  events(machineId: MachineId, afterSequence: number | null, limit: number): Promise<MachineEventPage> { return this.#call("MACHINES_EVENTS", { machineId, afterSequence, limit }).then(asPublic<EventsOut, MachineEventPage>); }
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt> { return this.#call("MACHINES_USAGE", { machineId, startUnixMs, endUnixMs }).then(usageOut); }
  recover(idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call("OPERATIONS_RECOVER", { idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  recoverOperation(idempotencyKey: IdempotencyKey): Promise<OperationId> { return this.#call("OPERATIONS_RECOVER_ID", { idempotencyKey }).then(operationId); }
  inspectOperation(operationId: OperationId): Promise<OperationObservation> { return this.#call("OPERATIONS_INSPECT", { operationId }).then(asPublic<OperationOut, OperationObservation>); }
  cancel(operationId: OperationId): Promise<OperationObservation> { return this.#call("OPERATIONS_CANCEL", { operationId }).then(asPublic<OperationOut, OperationObservation>); }
  async *watchOperation(operationId: OperationId): AsyncIterable<OperationObservation> { for (const observation of await this.#call("OPERATIONS_WATCH", { operationId })) yield asPublic<OperationOut, OperationObservation>(observation); }
  async #call<Key extends keyof MachinesHttpRoutes>(key: Key, request: MachinesHttpRequest<MachinesHttpRoutes[Key]>): Promise<MachinesHttpResponse<MachinesHttpRoutes[Key]>> {
    await ensureMachinesWasm();
    const route = httpRoutes()[key];
    const payload = WasmSimulatedMachines.encodeHttpRequest(request);
    const response = await this.#fetcher(new URL(`v1/machines/${route}`, this.#endpoint), { method: "POST", headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" }, body: payload });
    const bytes = await boundedBytes(response, this.#maximum);
    let body: string;
    try { body = new TextDecoder("utf-8", { fatal: true }).decode(bytes); }
    catch { throw new MachinesTransportError("response is not valid UTF-8", response.status); }
    if (!response.ok) throw new MachinesTransportError(body || `HTTP ${response.status}`, response.status);
    try {
      // Rust uses null-prototype objects while projecting untrusted JSON.
      // Clone once at the transport boundary to retain the historical plain
      // object API without rebuilding the generated DTO graph in TypeScript.
      return structuredClone(WasmSimulatedMachines.decodeHttpResponse(route, body, payload));
    } catch (error) {
      throw new MachinesTransportError(`invalid ${route} response: ${error instanceof Error ? error.message : String(error)}`, response.status);
    }
  }
}

export class MachinesTransportError extends Error { constructor(message: string, readonly status: number) { super(message); } }
async function boundedBytes(response: Response, maximum: number): Promise<Uint8Array> {
  const reader = response.body?.getReader();
  if (reader === undefined) return new Uint8Array();
  const chunks: Uint8Array[] = []; let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maximum) { await reader.cancel().catch(() => undefined); throw new MachinesTransportError("response exceeds configured bound", response.status); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(total); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes;
}
