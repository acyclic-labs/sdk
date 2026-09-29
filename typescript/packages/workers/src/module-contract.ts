// Generated from acyclic-workers::MODULE_TYPESCRIPT_CONTRACT. Do not edit.
export interface WorkerJobContext {
  readonly jobId: string;
  /** Starts at 1 and increases when an accepted job is retried. */
  readonly attempt: number;
  readonly signal: AbortSignal;
}
export interface WorkerModule {
  fetch?(request: Request): Response | Promise<Response>;
  run?(input: Uint8Array, context: WorkerJobContext): Uint8Array | Promise<Uint8Array>;
}
