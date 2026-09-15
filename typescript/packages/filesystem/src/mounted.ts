import type { FsCheckout, MutationResult, TransactionOperation, TransactionResult } from "./contracts.js";

export interface MountedCheckout {
  readonly mountId: Uint8Array;
  readonly volumeId: Uint8Array;
  readonly path: string;
  readonly checkout: FsCheckout;
}

export interface MountedSnapshot {
  readonly mounts: readonly {
    readonly mountId: Uint8Array;
    readonly volumeId: Uint8Array;
    readonly path: string;
    readonly generationId: Uint8Array;
  }[];
}

export class CrossVolumeError extends Error {
  readonly code = "EXDEV" as const;

  constructor(operation: string) {
    super(`${operation} cannot cross mounted filesystem volumes`);
    this.name = "CrossVolumeError";
  }
}

/**
 * Pure path router over caller-owned checkout handles.
 *
 * The caller remains responsible for checkout lifetime and for proving each supplied mount/volume
 * identity belongs to that checkout; this view adds longest-prefix routing and EXDEV fencing only.
 * Identity-only operations stay on the originating checkout because a file ID has no mount prefix.
 */
export class MountedView {
  readonly #mounts: readonly MountedCheckout[];

  constructor(mounts: readonly MountedCheckout[]) {
    const normalized = mounts.map((mount) => ({
      ...mount,
      mountId: identity(mount.mountId, "mount"),
      volumeId: identity(mount.volumeId, "volume"),
      path: mountPath(mount.path),
    }));
    if (!normalized.some((mount) => mount.path === "/")) {
      throw new RangeError("a mounted view requires a root mount");
    }
    if (new Set(normalized.map((mount) => mount.path)).size !== normalized.length) {
      throw new RangeError("mounted view paths must be unique");
    }
    this.#mounts = Object.freeze(normalized.sort((left, right) => right.path.length - left.path.length));
  }

  applyTransaction(operations: readonly TransactionOperation[]): Promise<TransactionResult> {
    const routed = operations.map(operation => this.#routeOperation(operation));
    const mount = routed[0]?.mount ?? this.#mounts.find(candidate => candidate.path === "/")!;
    if (routed.some(value => value.mount !== mount)) throw new CrossVolumeError("transaction");
    return mount.checkout.applyTransaction(routed.map(value => value.operation));
  }

  lookupNoFollow(path: string) { const routed = this.#route(path); return routed.mount.checkout.lookupNoFollow(routed.path); }
  statNoFollow(path: string) { const routed = this.#route(path); return routed.mount.checkout.statNoFollow(routed.path); }
  readMetadata(path: string) { const routed = this.#route(path); return routed.mount.checkout.readMetadata(routed.path); }
  setMetadata(path: string, canonicalBytes: Uint8Array) { const routed = this.#route(path); return routed.mount.checkout.setMetadata(routed.path, canonicalBytes); }
  setAttributes(path: string, canonicalBytes: Uint8Array, logicalBytes?: bigint) { const routed = this.#route(path); return routed.mount.checkout.setAttributes(routed.path, canonicalBytes, logicalBytes); }
  readNamedAttribute(path: string, attributeClass: Parameters<FsCheckout["readNamedAttribute"]>[1], name: Uint8Array) { const routed = this.#route(path); return routed.mount.checkout.readNamedAttribute(routed.path, attributeClass, name); }
  listNamedAttributes(path: string, after: Parameters<FsCheckout["listNamedAttributes"]>[1], maximumEntries: number) { const routed = this.#route(path); return routed.mount.checkout.listNamedAttributes(routed.path, after, maximumEntries); }
  writeNamedAttribute(path: string, attributeClass: Parameters<FsCheckout["writeNamedAttribute"]>[1], name: Uint8Array, bytes: Uint8Array, mode: Parameters<FsCheckout["writeNamedAttribute"]>[4]) { const routed = this.#route(path); return routed.mount.checkout.writeNamedAttribute(routed.path, attributeClass, name, bytes, mode); }
  removeNamedAttribute(path: string, attributeClass: Parameters<FsCheckout["removeNamedAttribute"]>[1], name: Uint8Array) { const routed = this.#route(path); return routed.mount.checkout.removeNamedAttribute(routed.path, attributeClass, name); }
  readFileRange(path: string, offset: bigint, length: bigint) { const routed = this.#route(path); return routed.mount.checkout.readFileRange(routed.path, offset, length); }
  planFileExtents(path: string, offset: bigint, length: bigint, maximumSpans: number) { const routed = this.#route(path); return routed.mount.checkout.planFileExtents(routed.path, offset, length, maximumSpans); }
  seekFileExtent(path: string, offset: bigint, target: Parameters<FsCheckout["seekFileExtent"]>[2]) { const routed = this.#route(path); return routed.mount.checkout.seekFileExtent(routed.path, offset, target); }
  readSymbolicLink(path: string) { const routed = this.#route(path); return routed.mount.checkout.readSymbolicLink(routed.path); }
  readReparsePoint(path: string) { const routed = this.#route(path); return routed.mount.checkout.readReparsePoint(routed.path); }
  listDirectory(path: string, after: string | undefined, maximumEntries: number) { const routed = this.#route(path); return routed.mount.checkout.listDirectory(routed.path, after, maximumEntries); }
  listDirectoryRecords(path: string, after: string | undefined, maximumEntries: number) { const routed = this.#route(path); return routed.mount.checkout.listDirectoryRecords(routed.path, after, maximumEntries); }

  createFile(path: string, bytes: Uint8Array): Promise<MutationResult> {
    const routed = this.#route(path);
    return routed.mount.checkout.createFile(routed.path, bytes);
  }

  createDirectory(path: string): Promise<MutationResult> {
    const routed = this.#route(path);
    return routed.mount.checkout.createDirectory(routed.path);
  }

  createSymbolicLink(path: string, target: Uint8Array): Promise<MutationResult> { const routed = this.#route(path); return routed.mount.checkout.createSymbolicLink(routed.path, target); }
  createSpecial(path: string, kind: Parameters<FsCheckout["createSpecial"]>[1]): Promise<MutationResult> { const routed = this.#route(path); return routed.mount.checkout.createSpecial(routed.path, kind); }
  createDevice(path: string, kind: Parameters<FsCheckout["createDevice"]>[1], major: number, minor: number): Promise<MutationResult> { const routed = this.#route(path); return routed.mount.checkout.createDevice(routed.path, kind, major, minor); }
  createReparsePoint(path: string, payload: Uint8Array): Promise<MutationResult> { const routed = this.#route(path); return routed.mount.checkout.createReparsePoint(routed.path, payload); }
  writeFile(path: string, offset: bigint, bytes: Uint8Array): Promise<MutationResult> { const routed = this.#route(path); return routed.mount.checkout.writeFile(routed.path, offset, bytes); }

  remove(path: string, expectedFileId?: Uint8Array): Promise<MutationResult> {
    const routed = this.#route(path);
    return routed.mount.checkout.remove(routed.path, expectedFileId);
  }

  rename(source: string, destination: string, replace: boolean): Promise<MutationResult> {
    const from = this.#route(source);
    const to = this.#route(destination);
    if (from.mount !== to.mount) throw new CrossVolumeError("rename");
    return from.mount.checkout.rename(from.path, to.path, replace);
  }

  hardLink(source: string, destination: string): Promise<MutationResult> {
    const from = this.#route(source);
    const to = this.#route(destination);
    if (from.mount !== to.mount) throw new CrossVolumeError("hard link");
    return from.mount.checkout.hardLink(from.path, to.path);
  }

  resizeFile(path: string, logicalBytes: bigint): Promise<MutationResult> { const routed = this.#route(path); return routed.mount.checkout.resizeFile(routed.path, logicalBytes); }
  zeroFileRange(path: string, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<MutationResult> { const routed = this.#route(path); return routed.mount.checkout.zeroFileRange(routed.path, offset, length, allocated, extend); }
  preallocateFile(path: string, offset: bigint, length: bigint, keepSize: boolean): Promise<MutationResult> { const routed = this.#route(path); return routed.mount.checkout.preallocateFile(routed.path, offset, length, keepSize); }
  cloneFileRange(source: string, sourceOffset: bigint, destination: string, destinationOffset: bigint, length: bigint): Promise<MutationResult> {
    const from = this.#route(source); const to = this.#route(destination);
    if (from.mount !== to.mount) throw new CrossVolumeError("clone range");
    return from.mount.checkout.cloneFileRange(from.path, sourceOffset, to.path, destinationOffset, length);
  }

  /**
   * Builds independent immutable candidates for every mount. This relies on the FsCheckout
   * contract that checkpointing neither publishes nor mutates pending checkout state.
   */
  async checkpointSnapshot(): Promise<MountedSnapshot> {
    return {
      mounts: await Promise.all(this.#mounts.map(async (mount) => ({
        mountId: mount.mountId.slice(),
        volumeId: mount.volumeId.slice(),
        path: mount.path,
        generationId: (await mount.checkout.checkpoint()).generationId.slice(),
      }))),
    };
  }

  #route(path: string): { readonly mount: MountedCheckout; readonly path: string } {
    const canonical = namespacePath(path);
    const mount = this.#mounts.find((candidate) => candidate.path === "/"
      || canonical === candidate.path
      || canonical.startsWith(`${candidate.path}/`));
    if (mount === undefined) throw new RangeError("path is outside the mounted view");
    const routed = mount.path === "/" ? canonical : canonical.slice(mount.path.length) || "/";
    return { mount, path: routed };
  }

  #routeOperation(operation: TransactionOperation): { readonly mount: MountedCheckout; readonly operation: TransactionOperation } {
    if (operation.kind === "rename" || operation.kind === "hard-link" || operation.kind === "clone-range") {
      const source = this.#route(operation.source); const destination = this.#route(operation.destination);
      if (source.mount !== destination.mount) throw new CrossVolumeError(operation.kind);
      return { mount: source.mount, operation: { ...operation, source: source.path, destination: destination.path } };
    }
    const routed = this.#route(operation.path);
    return { mount: routed.mount, operation: { ...operation, path: routed.path } };
  }
}

function identity(value: Uint8Array, label: string): Uint8Array {
  if (value.byteLength !== 16) throw new RangeError(`${label} identity must be exactly 16 bytes`);
  return value.slice();
}

function mountPath(value: string): string {
  const path = namespacePath(value);
  return path === "/" ? path : path.replace(/\/+$/, "");
}

function namespacePath(value: string): string {
  if (!value.startsWith("/")) throw new RangeError("mounted paths must be absolute");
  if (value.includes("\\") || value.includes("//") || (value !== "/" && value.endsWith("/")) || value.split("/").some((part) => part === "." || part === "..")) {
    throw new RangeError("mounted paths must be canonical");
  }
  return value;
}
