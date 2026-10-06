/** Runtime and packaged-artifact resolution for the cross-platform Objects client. */

import {
  isRustOwnedTransportUnavailable,
  OBJECTS_REMOTE_POLICY,
  selectRustOwnedTransport,
  type RustOwnedTransportAvailability,
  type RustOwnedTransportKind,
} from "./generated-client.js";

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
  let wasmAvailable = typeof WebAssembly === "object";
  const fallbacks: ("grpc-to-http" | "wasm-unavailable")[] = [];
  const installed: RustOwnedTransportAvailability = { http: false };
  if (policyRuntime === "native") {
    try {
      // Loading the optional native adapter is host plumbing only. The Rust
      // generated policy below still makes the default/fallback decision.
      await import("./v2-grpc.js");
      installed.grpc = true;
    } catch (error) {
      if (!isRustOwnedTransportUnavailable(error)) throw error;
      fallbacks.push("grpc-to-http");
    }
  }
  if (wasmAvailable) {
    try {
      // Check the artifact shipped by this package, rather than inferring
      // support from a runtime global alone. This catches incomplete installs
      // and browser bundles whose WASM asset was omitted.
      const { ensureObjectsWasm } = await import("./wasm-runtime.js");
      await ensureObjectsWasm();
      installed.http = true;
    } catch {
      wasmAvailable = false;
    }
  }
  if (!installed.http) fallbacks.push("wasm-unavailable");
  const transport = selectRustOwnedTransport(OBJECTS_REMOTE_POLICY, policyRuntime, undefined, installed) as ObjectsResolvedTransport;

  return {
    runtime,
    transport,
    wasm: { available: wasmAvailable, artifact: wasmArtifact },
    fallbacks,
  };
}
