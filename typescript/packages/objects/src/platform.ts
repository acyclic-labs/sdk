/** Runtime and packaged-artifact resolution for the cross-platform Objects client. */

export type ObjectsRuntime = "node" | "bun" | "browser" | "unknown";
export type ObjectsResolvedTransport = "grpc" | "http";

export interface ObjectsPlatformResolution {
  readonly runtime: ObjectsRuntime;
  readonly transport: ObjectsResolvedTransport;
  readonly wasm: {
    readonly available: boolean;
    readonly artifact: "generated/wasm/acyclic_objects_wasm_bg.wasm";
  };
  /** Empty when the preferred runtime path is available; otherwise explains the fallback. */
  readonly fallbacks: readonly ("grpc-to-http" | "wasm-unavailable")[];
}

const wasmArtifact = "generated/wasm/acyclic_objects_wasm_bg.wasm" as const;

export function detectObjectsRuntime(): ObjectsRuntime {
  const value = globalThis as typeof globalThis & {
    process?: { versions?: { bun?: string; node?: string } };
    window?: unknown;
  };
  if (typeof value.process?.versions?.bun === "string") return "bun";
  if (typeof value.process?.versions?.node === "string") return "node";
  if (value.window !== undefined) return "browser";
  return "unknown";
}

export function objectsWasmArtifactUrl(): URL {
  return new URL("../generated/wasm/acyclic_objects_wasm_bg.wasm", import.meta.url);
}

/**
 * Resolve the packaged Rust boundary without feature flags or platform-specific
 * binding choices. Native runtimes prefer gRPC when the published module is
 * loadable; every other runtime uses the packaged HTTP/WASM path.
 */
export async function resolveObjectsPlatform(): Promise<ObjectsPlatformResolution> {
  const runtime = detectObjectsRuntime();
  const wasmAvailable = typeof WebAssembly === "object";
  const fallbacks: ("grpc-to-http" | "wasm-unavailable")[] = [];
  let transport: ObjectsResolvedTransport = "http";

  if (!wasmAvailable) fallbacks.push("wasm-unavailable");
  if (runtime === "node" || runtime === "bun") {
    try {
      await import("./v2-grpc.js");
      transport = "grpc";
    } catch {
      fallbacks.push("grpc-to-http");
    }
  }

  return {
    runtime,
    transport,
    wasm: { available: wasmAvailable, artifact: wasmArtifact },
    fallbacks,
  };
}
