import type { FsTransaction, RawWorkspaceTransaction, TransactionRebaseResult } from "./contracts.js";
import { parseWorkspaceCommit } from "./workspace-results.js";

/** Shared transaction facade over the native and browser bindings. */
export function adaptTransaction(
  raw: RawWorkspaceTransaction,
  decodeRebase: (value: TransactionRebaseResult) => TransactionRebaseResult,
): FsTransaction {
  return {
    createDirAll(path) { return raw.createDirAll(path); },
    createDirectory(path) { return raw.createDirectory(path); },
    createSymbolicLink(path, target) { return raw.createSymbolicLink(path, target); },
    write(path, bytes) { return raw.write(path, bytes); },
    remove(path) { return raw.remove(path); },
    copy(source, destination) { return raw.copy(source, destination); },
    rename(source, destination) { return raw.rename(source, destination); },
    hardLink(source, destination) { return raw.hardLink(source, destination); },
    writeRange(path, offset, bytes) {
      requireNonnegative(offset, "write offset");
      return raw.writeRange(path, offset, bytes);
    },
    resize(path, logicalBytes) {
      requireNonnegative(logicalBytes, "logical bytes");
      return raw.resize(path, logicalBytes);
    },
    zeroRange(path, offset, length, allocated, extend) {
      requireNonnegative(offset, "zero-range offset");
      requireNonnegative(length, "zero-range length");
      return raw.zeroRange(path, offset, length, allocated, extend);
    },
    preallocate(path, offset, length, keepSize) {
      requireNonnegative(offset, "preallocation offset");
      requireNonnegative(length, "preallocation length");
      return raw.preallocate(path, offset, length, keepSize);
    },
    cloneRange(source, sourceOffset, destination, destinationOffset, length) {
      requireNonnegative(sourceOffset, "clone source offset");
      requireNonnegative(destinationOffset, "clone destination offset");
      requireNonnegative(length, "clone length");
      return raw.cloneRange(source, sourceOffset, destination, destinationOffset, length);
    },
    async rebase(maximumConflicts) {
      requirePositiveInteger(maximumConflicts, "maximum transaction conflicts");
      return decodeRebase(await raw.rebase(maximumConflicts));
    },
    async commit() { return parseWorkspaceCommit(await raw.commit()); },
    async close(): Promise<void> {},
  };
}

function requireNonnegative(value: bigint, label: string): void {
  if (value < 0n) throw new RangeError(`${label} must be non-negative`);
}

function requirePositiveInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
}
