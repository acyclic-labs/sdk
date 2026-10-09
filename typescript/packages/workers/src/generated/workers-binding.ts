// Generated from Rust package identity and canonical failure DTO. Do not edit.
import type { WorkersFailure } from "./semantic/workers/WorkersFailure.js";
export const WORKERS_BINDING_VERSION = "0.2.0";
export const MAX_MESSAGE_BYTES = 16777216;
export function workersCancelledFailure(): WorkersFailure {
  const metadata = {"code":"cancelled","message":"Workers operation cancelled","rawDetails":[]};
  return { ...metadata, rawDetails: Uint8Array.from(metadata.rawDetails) };
}
