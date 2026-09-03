/** Portable execution request and result. */
export interface ExecutionRequest { readonly program: string; readonly args: readonly string[] }
export interface ExecutionResult { readonly code: number; readonly stdout: Uint8Array; readonly stderr: Uint8Array }
/** Public execution placement contract. */
export interface MachinesProvider { execute(request: ExecutionRequest): Promise<ExecutionResult> }
/** Deterministic execution simulator; it does not run operating-system processes. */
export class SimulatedMachines implements MachinesProvider {
  readonly #results: ExecutionResult[];
  constructor(results: readonly ExecutionResult[] = []) { this.#results = [...results]; }
  async execute(request: ExecutionRequest): Promise<ExecutionResult> { return this.#results.shift() ?? { code: 0, stdout: new TextEncoder().encode(`${request.program} ${request.args.join(" ")}`), stderr: new Uint8Array() }; }
}
