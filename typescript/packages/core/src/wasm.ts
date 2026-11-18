/** Resolve packaged WASM bytes for file URLs and leave network URLs to the binding. */
export async function wasmSource(url: URL): Promise<URL | Uint8Array<ArrayBuffer>> {
  if (url.protocol !== "file:") return url;
  const { readFile } = await import("node:fs/promises");
  return Uint8Array.from(await readFile(url));
}

/** Read a packaged WASM file synchronously when the runtime exposes its filesystem. */
export function wasmSourceSync(url: URL): Uint8Array | undefined {
  const runtime = globalThis as typeof globalThis & { process?: { getBuiltinModule?: (name: string) => unknown } };
  const filesystem = runtime.process?.getBuiltinModule?.("node:fs") as { readFileSync?: (path: URL) => Uint8Array } | undefined;
  return filesystem?.readFileSync?.(url);
}