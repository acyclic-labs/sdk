/** Immutable filesystem generation. */
export interface Generation { readonly id: string; readonly files: ReadonlyMap<string, Uint8Array> }
/** Public filesystem provider contract. */
export interface FilesystemProvider {
  create(): Promise<Generation>;
  get(id: string): Promise<Generation>;
  write(base: string, path: string, value: Uint8Array): Promise<Generation>;
  join(base: string, children: readonly string[]): Promise<Generation>;
}

/** Deterministic process-local filesystem provider. */
export class MemoryFilesystem implements FilesystemProvider {
  readonly #values = new Map<string, Generation>();
  #next = 0;
  async create(): Promise<Generation> { return this.#save(new Map()); }
  async get(id: string): Promise<Generation> {
    const value = this.#values.get(id);
    if (!value) throw new Error(`generation not found: ${id}`);
    return cloneGeneration(value);
  }
  async write(base: string, path: string, value: Uint8Array): Promise<Generation> {
    const files = new Map((await this.get(base)).files);
    files.set(path, value.slice());
    return this.#save(files);
  }
  async join(base: string, children: readonly string[]): Promise<Generation> {
    const baseline = (await this.get(base)).files;
    const files = new Map(baseline);
    const changes = new Map<string, Uint8Array>();
    for (const child of children) for (const [path, value] of (await this.get(child)).files) {
      const initial = baseline.get(path);
      if (initial && bytesEqual(initial, value)) continue;
      const previous = changes.get(path);
      if (previous && !bytesEqual(previous, value)) throw new Error(`conflict: ${path}`);
      changes.set(path, value); files.set(path, value);
    }
    return this.#save(files);
  }
  #save(files: ReadonlyMap<string, Uint8Array>): Generation {
    const value = { id: `memory-generation-${this.#next++}`, files: new Map(files) };
    this.#values.set(value.id, cloneGeneration(value)); return cloneGeneration(value);
  }
}
const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => left.length === right.length && left.every((value, index) => value === right[index]);
const cloneGeneration = (generation: Generation): Generation => ({ id: generation.id, files: new Map(Array.from(generation.files, ([path, value]) => [path, value.slice()])) });
