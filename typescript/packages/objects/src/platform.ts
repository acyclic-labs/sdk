/** Runtime and packaged-artifact resolution for the cross-platform Objects client. */

import { isRustOwnedTransportUnavailable, OBJECTS_REMOTE_POLICY, selectRustOwnedTransport, type RustOwnedTransportKind } from "./generated-client.js";

export type ObjectsRuntime = "node" | "bun" | "browser" | "unknown";
export type ObjectsResolvedTransport = Extract<RustOwnedTransportKind, "grpc" | "http">;

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
  const policyRuntime = runtime === "node" || runtime === "bun" ? "native" : "browser";
  const policyOptions = OBJECTS_REMOTE_POLICY.transport[policyRuntime];
  let wasmAvailable = typeof WebAssembly === "object";
  const fallbacks: ("grpc-to-http" | "wasm-unavailable")[] = [];
  let transport: ObjectsResolvedTransport = selectRustOwnedTransport(OBJECTS_REMOTE_POLICY, policyRuntime, undefined, { grpc: true, http: true }) as ObjectsResolvedTransport;

  if (wasmAvailable) {
    try {
      // Check the artifact shipped by this package, rather than inferring
      // support from a runtime global alone. This catches incomplete installs
      // and browser bundles whose WASM asset was omitted.
      const { ensureObjectsWasm } = await import("./wasm-runtime.js");
      await ensureObjectsWasm();
    } catch {
      wasmAvailable = false;
      fallbacks.push("wasm-unavailable");
    }
  } else {
    fallbacks.push("wasm-unavailable");
  }
  if (transport === "grpc") {
    try {
      await import("./v2-grpc.js");
    } catch (error) {
      if (!isRustOwnedTransportUnavailable(error)) throw error;
      fallbacks.push("grpc-to-http");
      transport = policyOptions.find(option => option.kind === "http") === undefined ? "grpc" : "http";
    }
  }

  return {
    runtime,
    transport,
    wasm: { available: wasmAvailable, artifact: wasmArtifact },
    fallbacks,
  };
}
