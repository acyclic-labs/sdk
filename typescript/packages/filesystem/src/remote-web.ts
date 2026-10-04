type FilesystemWasm = typeof import("../generated/wasm/acyclic_fs_wasm.js");

let binding: Promise<FilesystemWasm> | undefined;

async function loadBinding(): Promise<FilesystemWasm> {
  binding ??= (async () => {
    const module = await import("../generated/wasm/acyclic_fs_wasm.js");
    const nodeVersion = (globalThis as { process?: { versions?: { node?: string } } }).process?.versions?.node;
    if (typeof nodeVersion !== "string") {
      await module.default();
    } else {
      const fsModule: string = "node:fs/promises";
      const { readFile } = await import(fsModule) as { readFile(url: URL): Promise<Uint8Array> };
      await module.default({ module_or_path: await readFile(new URL("../generated/wasm/acyclic_fs_wasm_bg.wasm", import.meta.url)) });
    }
    return module;
  })();
  return binding;
}

/** Validate one hosted filesystem bearer credential through Rust/WASM. */
export async function validateFilesystemCredential(token: string): Promise<void> {
  const module = await loadBinding();
  try {
    module.validate_remote_web_credential(token);
  } catch (error) {
    throw new TypeError(String(error));
  }
}
