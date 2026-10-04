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
 * Opens the filesystem companion's native process owner lazily. Importing
 * GraphCoder must not load native bindings or start a worker.
 */
export async function openDefaultNodeProcessOwner(): Promise<OwnedProcessOwner> {
  const moduleName = "@acyclic-labs/fs/native";
  const nativeModule = await import(moduleName) as {
    readonly openNativeProcessIo?: () => Promise<unknown>;
    readonly createNativeProcessOwnerAdapter?: (io: unknown) => OwnedProcessOwner;
  };
  if (typeof nativeModule.openNativeProcessIo !== "function"
    || typeof nativeModule.createNativeProcessOwnerAdapter !== "function") {
    throw new Error("@acyclic-labs/fs/native does not export the native process owner adapter");
  }
  return nativeModule.createNativeProcessOwnerAdapter(await nativeModule.openNativeProcessIo());
}

/** Compose the local bridge with the filesystem companion's owned process boundary. */
export async function createNativeNodeGraphCoderConnection(
  options: Omit<GraphCoderProcessBridgeOptions, "processOwner">,
): Promise<NodeGraphCoderConnection> {
  const processOwner = await openDefaultNodeProcessOwner();
  return createNodeGraphCoderConnection({ ...options, processOwner });
}
