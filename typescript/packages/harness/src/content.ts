import { type FileRef, type Limits, type VolumeRef } from "./conversation.js";
import { type ResourceRef, type ForkParentSnapshot, type ForkPublisher, type ForkReport } from "./fork.js";
import { type NativeContracts } from "./native-contracts.js";
import { type OperationId, type Scope } from "./index.js";
import { type WasmReducer } from "../generated/wasm/acyclic_harness_wasm.js";
export interface ContentBindings {
  /** Exact native-contract validation before reading or publishing a ref. */
  readonly validate: (file: FileRef, limits: Limits) => void;
  readonly read: (file: FileRef) => Promise<Uint8Array>;
  readonly fileReadCapability: (file: FileRef) => string;
  readonly volumeReadCapability: (volume: VolumeRef) => string;
  readonly directoryReadCapability: (volume: VolumeRef, prefix: string) => string;
  readonly directory?: PrivateDirectoryBindings;
  readonly writer?: Readonly<{
    readonly volume: VolumeRef;
    readonly writeCapability: () => string;
    readonly stage: (operationId: string, path: string, bytes: Uint8Array, mediaType: string, displayName: string) => Promise<FileRef>;
  }>;
}
export interface PrivateDirectoryPage {
  readonly generation: ResourceRef<"generation">;
  readonly entries: readonly Readonly<{ name: string; kind: "file" | "directory" }>[];
  readonly hasMore: boolean;
}
export interface PrivateDirectoryBindings {
  list(volume: VolumeRef<"agent_private">, grantedPrefix: string, path: string,
    expectedGeneration: ResourceRef<"generation"> | null, after: string | null, maximumEntries: number): Promise<PrivateDirectoryPage>;
  readPath(volume: VolumeRef<"agent_private">, grantedPrefix: string, path: string,
    expectedGeneration: ResourceRef<"generation"> | null): Promise<Readonly<{ file: FileRef; bytes: Uint8Array }>>;
}
export type ContentReader = Pick<ContentBindings,
  "validate" | "read" | "fileReadCapability" | "volumeReadCapability" | "directoryReadCapability" | "directory">;
export interface ContentMountResolver {
  mount(volume: VolumeRef): ContentReader & Readonly<{
    requireFileRead(file: FileRef): void;
    requireDirectoryPath(volume: VolumeRef<"agent_private">, grantedPrefix: string, path: string): void;
  }>
}

function pinContentReader(source: ContentReader): ContentReader {
  return Object.freeze({
    validate: source.validate.bind(source),
    read: source.read.bind(source),
    fileReadCapability: source.fileReadCapability.bind(source),
    volumeReadCapability: source.volumeReadCapability.bind(source),
    directoryReadCapability: source.directoryReadCapability.bind(source),
    ...(source.directory === undefined ? {} : { directory: Object.freeze({
      list: source.directory.list.bind(source.directory),
      readPath: source.directory.readPath.bind(source.directory),
    }) }),
  });
}

export function pinContentBindings(contracts: NativeContracts, source: ContentBindings): ContentBindings {
  const writer = source.writer;
  return Object.freeze({
    ...pinContentReader(source),
    ...(writer === undefined ? {} : { writer: Object.freeze({
      volume: contracts.validate("volume_ref", writer.volume),
      writeCapability: writer.writeCapability.bind(writer),
      stage: writer.stage.bind(writer),
    }) }),
  });
}

export function composeContentBindings(contracts: NativeContracts, sources: readonly Readonly<{
  volume: VolumeRef; content: ContentBindings;
}>[], mount?: ContentMountResolver): ContentBindings {
  if (sources.length === 0 && mount === undefined) throw new TypeError("at least one content volume or mount is required");
  const volumes = new Map<string, ContentBindings>();
  let writer: ContentBindings["writer"];
  const decoder = new TextDecoder();
  const key = (volume: VolumeRef): string => {
    const checked = contracts.validate("volume_ref", volume);
    return decoder.decode(contracts.encodeCanonicalJson(checked));
  };
  for (const source of sources) {
    const id = key(source.volume);
    if (volumes.has(id)) throw new TypeError("content volume is registered twice");
    const content = pinContentBindings(contracts, source.content);
    volumes.set(id, content);
    if (content.writer !== undefined) {
      if (writer !== undefined || key(content.writer.volume) !== id) {
        throw new TypeError("content writer is ambiguous or bound to another volume");
      }
      writer = content.writer;
    }
  }
  const owner = (volume: VolumeRef, file?: FileRef): ContentReader => {
    const binding = volumes.get(key(volume));
    if (binding) return binding;
    if (!mount) throw new Error("content volume is not registered");
    const mounted = mount.mount(volume);
    if (file !== undefined) mounted.requireFileRead(file);
    return pinContentReader(mounted);
  };
  const directoryOwner = (volume: VolumeRef<"agent_private">, prefix: string, path: string): ContentReader => {
    const binding = volumes.get(key(volume));
    if (binding) return binding;
    if (!mount) throw new Error("content volume is not registered");
    const mounted = mount.mount(volume);
    mounted.requireDirectoryPath(volume, prefix, path);
    return pinContentReader(mounted);
  };
  return Object.freeze({
    validate: (file: FileRef, limits: Limits) => owner(file.volume, file).validate(file, limits),
    read: async (file: FileRef) => owner(file.volume, file).read(file),
    fileReadCapability: (file: FileRef) => owner(file.volume, file).fileReadCapability(file),
    volumeReadCapability: (volume: VolumeRef) => owner(volume).volumeReadCapability(volume),
    directoryReadCapability: (volume: VolumeRef, prefix: string) => owner(volume).directoryReadCapability(volume, prefix),
    directory: Object.freeze({
      list: async (volume: VolumeRef<"agent_private">, prefix: string, path: string,
        generation: ResourceRef<"generation"> | null, after: string | null, maximum: number) => {
        const reader = directoryOwner(volume, prefix, path).directory;
        if (!reader) throw new Error("private directory provider is not bound");
        return reader.list(volume, prefix, path, generation, after, maximum);
      },
      readPath: async (volume: VolumeRef<"agent_private">, prefix: string, path: string,
        generation: ResourceRef<"generation"> | null) => {
        const reader = directoryOwner(volume, prefix, path).directory;
        if (!reader) throw new Error("private directory provider is not bound");
        return reader.readPath(volume, prefix, path, generation);
      },
    }),
    ...(writer === undefined ? {} : { writer }),
  });
}

export function pinForkParentSnapshot(snapshot: ForkParentSnapshot): ForkParentSnapshot {
  if (snapshot.parent.kind !== "conversation" || typeof snapshot.parent.id !== "string" || !snapshot.parent.id
    || /[\\/\x00-\x1f]/.test(snapshot.parent.id) || snapshot.parent.id === "." || snapshot.parent.id === ".."
    || typeof snapshot.revision !== "bigint" || snapshot.revision <= 0n) {
    throw new TypeError("fork snapshot needs a bound parent conversation revision");
  }
  return Object.freeze({ parent: Object.freeze({ ...snapshot.parent }), revision: snapshot.revision });
}

export function pinForkPublisher(contracts: NativeContracts, value: ForkPublisher): ForkPublisher {
  const parent = value.parent();
  if (parent.kind !== "conversation" || typeof parent.id !== "string" || !parent.id
    || /[\\/\x00-\x1f]/.test(parent.id) || parent.id === "." || parent.id === "..") {
    throw new TypeError("fork publisher needs a bound parent conversation");
  }
  const pinned = Object.freeze({ ...parent });
  const assertPinned = (): void => {
    if (!contracts.canonicalEqual(value.parent(), pinned)) {
      throw new Error("fork publisher parent changed after binding");
    }
  };
  return Object.freeze({
    parent: () => pinned,
    spawnFromReport: (report: ForkReport) => { assertPinned(); return value.spawnFromReport(report); },
    reconcileSpawn: (operationId: OperationId) => { assertPinned(); return value.reconcileSpawn(operationId); },
  });
}
export interface InheritedModelPrefix {
  readonly core: WasmReducer;
  readonly scope: Scope;
  readonly head: FileRef;
  readonly files: ReadonlyMap<string, Uint8Array>;
}
