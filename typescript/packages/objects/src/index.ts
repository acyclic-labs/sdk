/** Immutable object reference. */
export interface ObjectRef { readonly version: string }
/** Public immutable object provider contract. */
export interface ObjectsProvider { put(value: Uint8Array): Promise<ObjectRef>; get(reference: ObjectRef): Promise<Uint8Array> }
/** Process-local immutable object provider. */
export class MemoryObjects implements ObjectsProvider {
  readonly #values = new Map<string, Uint8Array>();
  async put(value: Uint8Array): Promise<ObjectRef> {
    const bytes = new Uint8Array(await crypto.subtle.digest("SHA-256", Uint8Array.from(value).buffer));
    const version = Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
    this.#values.set(version, value.slice()); return { version };
  }
  async get(reference: ObjectRef): Promise<Uint8Array> {
    const value = this.#values.get(reference.version);
    if (!value) throw new Error(`object not found: ${reference.version}`);
    return value.slice();
  }
}
