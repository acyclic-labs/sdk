export * from "./process.js";

import { HarnessGraphCoderTransport } from "./bridge.js";
import { JsonLineGraphCoderBridge, type GraphCoderProcessBridgeOptions } from "./process.js";
import type { OwnedProcessOwner } from "./owned-process.js";

/** A host-owned Node connection over the durable local runtime bridge. */
export interface NodeGraphCoderConnection {
  /** The concrete owner-controlled process bridge exposes close/waitForExit. */
  readonly bridge: JsonLineGraphCoderBridge;
  readonly transport: HarnessGraphCoderTransport;
}

/**
 * Compose the explicit process bridge and the transport adapter for a local
 * runtime executable. The caller owns the returned bridge lifecycle.
 */
export function createNodeGraphCoderConnection(options: GraphCoderProcessBridgeOptions): NodeGraphCoderConnection {
  const bridge = new JsonLineGraphCoderBridge(options);
  return Object.freeze({ bridge, transport: new HarnessGraphCoderTransport(bridge) });
}

/**
 * Opens the installed filesystem companion's process owner. The import is
 * deliberately lazy so importing GraphCoder does not initialize native
 * filesystem code or start any workers.
 */
export async function openDefaultNodeProcessOwner(): Promise<OwnedProcessOwner> {
  const moduleName = "@acyclic-labs/fs/native";
  const nativeModule = await import(moduleName) as {
    readonly openNativeProcessOwner?: () => Promise<{
      readonly io?: unknown;
    }>;
    readonly createNativeProcessOwnerAdapter?: (io: unknown) => OwnedProcessOwner;
  };
  if (typeof nativeModule.openNativeProcessOwner !== "function"
    || typeof nativeModule.createNativeProcessOwnerAdapter !== "function") {
    throw new Error("@acyclic-labs/fs/native does not export the native process owner adapter");
  }
  const nativeOwner = await nativeModule.openNativeProcessOwner();
  if (nativeOwner.io === undefined) {
    throw new Error("the native filesystem companion does not provide streaming process ownership");
  }
  return nativeModule.createNativeProcessOwnerAdapter(nativeOwner.io);
}

/**
 * Local production composition. It binds the bridge to the filesystem
 * companion's native process owner before the first runtime process starts.
 */
export async function createNativeNodeGraphCoderConnection(
  options: Omit<GraphCoderProcessBridgeOptions, "processOwner">,
): Promise<NodeGraphCoderConnection> {
  const processOwner = await openDefaultNodeProcessOwner();
  return createNodeGraphCoderConnection({ ...options, processOwner });
}
