// The shipping acyclic file module. Configuration records come from the kernel.
export type {
  Admission, AgentConfig, Config, PluginEntry, PluginManifest, PluginSource,
  Plugins, Policy, Rule,
} from './config';

export type Path = string;
/** Opaque immutable file or namespace selection; pass it back unchanged. */
export type Version = string & { readonly __brand: 'Version' };
/** The version of the last directory entry delivered by a listing. */
export type Cursor = string & { readonly __brand: 'Cursor' };

export interface File<T = unknown> {
  readonly path: Path;
  readonly version: Version;
  readonly size: number;
  text(): string;
  /** Zero-based lines in the retained text; performs no host call. */
  lines(from: number, count: number): string[];
  json(): T;
}
export interface Entry {
  readonly name: string;
  readonly path: Path;
  readonly kind: 'file' | 'dir';
  readonly version: Version;
}
export interface Listing<T> extends AsyncIterable<T> {
  readonly cursor: Cursor;
}
/** The original conditional publication failed; current is diagnostic only. */
export class ConflictError extends Error {
  constructor(path: Path, current: File | null);
  readonly name: 'ConflictError';
  readonly path: Path;
  readonly current: File | null;
}
export class DeniedError extends Error {
  constructor(reason: string);
  readonly name: 'DeniedError';
  readonly reason: string;
}

/** Retains the complete immutable text; null when the authorized file is absent. */
export function read<T = unknown>(path: Path): Promise<File<T> | null>;
/** Objects are JSON; a .ts destination stores an export-default module. */
export function write(path: Path, data: string | object, ifVersion: Version | 'absent' | 'any'): Promise<Version>;
export function remove(path: Path, ifVersion: Version | 'any'): Promise<void>;
/** Directory entries at one immutable generation, fetched only as consumed. */
export function list(path: Path, after?: Cursor): Listing<Entry>;
