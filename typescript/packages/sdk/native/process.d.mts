import type {Readable} from 'node:stream';

export interface ProcessCompletion {
  schema: 'acyclic.process-custody-completion.v3';
  invocation: string;
  platform: string;
  host_exit_code: number;
  root_status: {native: number; code: number | null; signal: number | null} | null;
  cancelled: boolean;
  process_tree_retired: boolean;
  failure: {phase: string; error: {kind: string; raw_os_error: number | null}} | null;
}
export interface ProcessResult {
  code: number | null;
  signal: NodeJS.Signals | null;
  completion?: ProcessCompletion;
  hostStatus?: {code: number | null; signal: NodeJS.Signals | null};
  failures: {phase: string; error: unknown; stream?: 'stdout' | 'stderr'}[];
}
export interface ProcessOptions {
  /** Absolute path to the installed acyclic-process-host executable. */
  host: string;
  /** The caller owns cancellation and any deadline. Cancellation retires the tree. */
  signal?: AbortSignal;
  input?: string | NodeJS.ArrayBufferView | Readable;
  env?: NodeJS.ProcessEnv;
  cwd?: string | URL;
  /** Standard streams are inherited through the native owner. Extra descriptors and IPC are unsupported. */
  stdio?: import('node:child_process').StdioOptions;
  /** Accepted writes must settle; cancellation remains armed through their join. */
  onOutput?: (stream: 'stdout' | 'stderr', bytes: Buffer, active: boolean) => void | Promise<void>;
  onClose?: (result: Omit<ProcessResult, 'failures'>) => void;
  /** Nonzero exits and failures can precede accepted output completion. Returned work is joined. */
  onTerminal?: (result: {exitCode: number | null; signal: NodeJS.Signals | null; cause: string}) => void | Promise<void>;
}
export function commandProcess(program: string, args: string[], options: ProcessOptions): Promise<ProcessResult>;
export function commandCapture(program: string, args: string[],
  options: Omit<ProcessOptions, 'onOutput' | 'onClose' | 'onTerminal'> & {maxBuffer?: number}):
  Promise<{stdout: string; stderr: string}>;
export function withProcessSignals<T>(action: (signal: AbortSignal) => Promise<T>,
  options?: {signal?: AbortSignal; reason?: (signal: 'SIGINT' | 'SIGTERM') => unknown}): Promise<T>;
