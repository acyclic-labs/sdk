/** One Rust file fold and cursor follow; the platform provider owns all I/O. */
export {
  WasmFileClient as FileClient,
  type FileClientError,
  type FileClientLimits,
  type FileClientDurability,
  WasmClientQuery as ClientQuery,
  WasmQueryViews as QueryViews,
  WasmSwarmClient as SwarmClient,
  WasmClientViews as ClientViews,
  WasmTaskVolume as TaskVolume,
  WasmStream,
  WasmSessionAuthority,
  type Channel,
  type Cursor,
  type FileKey,
  type Generation,
  type Query,
  type QueryDelivery,
  type QueryFileDelta,
  type QueryFileInfo,
  type QueryViewLimits,
  type InterestDocument,
  type SwarmClientLimits,
  type ClientViewLimits,
  type SwarmDelivery,
  type SwarmStatusInfo,
  type SwarmStatusValue,
  type AttentionEvent,
  type AttentionNotification,
  type NotificationFileView,
  type NotificationQueryView,
  type CommandOutputView,
  type OutputOwner,
  type HeadlessOutcome,
  type TaskQueryInfo,
  type TaskTextInfo,
  type ToolQueryInfo,
  type TurnCompletionInfo,
  type ModelQueryView,
  type ModelFileView,
  type GoalQueryView,
  type GoalFileView,
  type GoalProgress,
  type GoalState,
  type GoalStatus,
  type ChildStatus,
} from "../generated/wasm/acyclic_harness_wasm.js";
export { ensureHarnessWasm as initializeClientQueries } from "./wasm-runtime.js";

/** Key for a latest value on `<root>/status`; publish parent membership first. */
export function swarmStatusKey(agent: string, key: Uint8Array): Uint8Array {
  const prefix = new TextEncoder().encode(agent);
  if (!agent || agent.includes("\0") || !key.length) throw new Error("Invalid swarm status identity");
  const result = new Uint8Array(prefix.length + 1 + key.length);
  result.set(prefix); result.set(key, prefix.length + 1);
  return result;
}
