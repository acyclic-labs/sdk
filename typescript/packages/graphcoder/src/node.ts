export * from "./process.js";

import { openNativeProcessOwner } from "@acyclic-labs/fs/native";
import { HarnessGraphCoderTransport } from "./bridge.js";
import { JsonLineGraphCoderBridge, type GraphCoderProcessBridgeOptions } from "./process.js";

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
 * Open a production Node connection only after the native process owner has
 * been loaded and capability-checked. No runtime process is spawned if the
 * companion is unavailable or lacks stable descendant ownership.
 */
export async function openNativeGraphCoderConnection(
  options: Omit<GraphCoderProcessBridgeOptions, "processOwner">,
): Promise<NodeGraphCoderConnection> {
  const processOwner = await openNativeProcessOwner();
  return createNodeGraphCoderConnection({ ...options, processOwner });
}
