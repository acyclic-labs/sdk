import type * as NativeBinding from "../generated/native/binding.js";

import type * as WasmBinding from "../generated/wasm/acyclic_fs_wasm.js";

import { type BrowserFsOptions, type MemoryFsOptions, type ObjectCacheStats, type GenerationExportManifest, type WorkspaceName, type JoinOptions, type ResidencyObservation, type PromotionRequest, type CheckoutOptions, type DeviceKind, type EmptySpecialKind, type NamedAttributeClass, type ExtentSeekTarget, type NamedAttributeWriteMode, type SpeculationOptions, type VolumeOptions, type EngineCapabilities } from "./contracts.js";

export type WasmBindings = Pick<
  typeof WasmBinding,
  | "default"
  | "encodeMergePlanJson"
  | "decodeMergePlanJson"
  | "encodeMergeCandidateJson"
  | "decodeMergeCandidateJson"
  | "encodeMultiRootPlanJson"
  | "decodeMultiRootPlanJson"
  | "encodeMultiRootCandidateJson"
  | "decodeMultiRootCandidateJson"
  | "encodePublicationJson"
  | "decodePublicationJson"
> & {
  openBrowserFs(options: BrowserFsOptions): Promise<WasmRawFs>;
  openMemoryFs(options: MemoryFsOptions): WasmRawFs;
  readonly BrowserWorkspaceContextRegistry: typeof WasmBinding.BrowserWorkspaceContextRegistry;
};

export interface RawWorkspaceContextRegistry {
  registerRoot(contextId: Uint8Array, rootsWire: Uint8Array): Promise<Uint8Array>;
  registerChild(
    contextId: Uint8Array,
    parentContextId: Uint8Array,
    rootsWire: Uint8Array,
  ): Promise<Uint8Array>;
  adoptRoot(contextId: Uint8Array, rootWire: Uint8Array): Promise<Uint8Array>;
  removeRoot(contextId: Uint8Array, rootId: Uint8Array): Promise<Uint8Array>;
  resolve(contextId: Uint8Array): Promise<Uint8Array>;
  setActive(contextId: Uint8Array, active: boolean): Promise<Uint8Array>;
  setWorkspace(
    contextId: Uint8Array,
    rootId: Uint8Array,
    workspaceId: Uint8Array,
    workspaceName: string,
    parentWorkspaceId?: Uint8Array,
  ): Promise<Uint8Array>;
  discardSubtree(
    parentContextId: Uint8Array,
    childContextId: Uint8Array,
    maximum: number,
  ): Promise<Uint8Array>;
}

type WasmTypedClass<Base, Args extends object = {}, AsyncResults extends object = {}, SyncResults extends object = {}, Properties extends object = {}> = Omit<{
  [Key in keyof Base]: Base[Key] extends (...args: infer Parameters) => infer Result
    ? (...args: Key extends keyof Args ? Args[Key] extends readonly unknown[] ? Args[Key] : never : Parameters) =>
      Key extends keyof SyncResults ? SyncResults[Key] : Key extends keyof AsyncResults ? Promise<AsyncResults[Key]> : Result
    : Key extends keyof Args | keyof AsyncResults | keyof SyncResults ? never : Base[Key];
}, keyof Properties> & Properties & (Exclude<keyof Args | keyof AsyncResults | keyof SyncResults | keyof Properties, keyof Base> extends never
  ? unknown : { readonly __unknownGeneratedWasmKeys__: Exclude<keyof Args | keyof AsyncResults | keyof SyncResults | keyof Properties, keyof Base> });

export type WasmRawMergeConflict = WasmBinding.MergeConflictResult;

export type WasmRawObjectCacheStats = { readonly [Key in keyof ObjectCacheStats]: string };

export type WasmImportManifest = Pick<GenerationExportManifest, "manifestBytes" | "objects">;

export type WasmRawGenerationDiff = WasmBinding.BrowserGenerationDiffResult;

export type WasmRawGeneration = WasmTypedClass<WasmBinding.BrowserGeneration, {
  listDirectory: [path: string, after: WorkspaceName | undefined, maximumEntries: number];
}, {
  pin: WasmRawGeneration;
}>;

export type WasmRawChangeSet = WasmTypedClass<WasmBinding.BrowserChangeSet, {}, {
  compose: WasmRawChangeSet;
}, {}, {
  readonly from: WasmRawGeneration;
  readonly to: WasmRawGeneration;
}>;

export type WasmRawJoinPlan = WasmBinding.BrowserJoinPlan;

export type WasmRawWorkspace = WasmTypedClass<WasmBinding.BrowserWorkspace, {
  joinInto: [target: WasmRawWorkspace, options: JoinOptions];
}, {
  checkpoint: WasmRawGeneration;
  generation: WasmRawGeneration;
  diff: WasmRawChangeSet;
  fork: WasmRawWorkspace;
  forkAt: WasmRawWorkspace;
  pin: WasmRawGeneration;
  sync: WasmRawGeneration;
}>;

export type WasmRawTransaction = WasmBinding.BrowserTransaction;

export type WasmRawSpeculation = WasmTypedClass<WasmBinding.BrowserSpeculation, {
  observe: [observation: ResidencyObservation];
  planPromotion: [request: PromotionRequest];
}>;

export type WasmRawVolume = WasmTypedClass<WasmBinding.BrowserVolume, {
  checkout: [options: CheckoutOptions];
}, {
  checkout: WasmRawCheckout;
}>;

export type WasmRawCheckout = WasmTypedClass<
  WasmBinding.BrowserCheckout,
  {
    createDevice: [path: string, kind: DeviceKind, major: number, minor: number];
    createSpecial: [path: string, kind: EmptySpecialKind];
    listDirectory: [path: string, after: string | undefined, maximumEntries: number];
    listDirectoryRecords: [path: string, after: string | undefined, maximumEntries: number];
    listNamedAttributes: [path: string, afterClass: NamedAttributeClass | undefined, afterName: Uint8Array | undefined, maximumEntries: number];
    remove: [path: string, expectedFileId: Uint8Array | undefined];
    seekFileExtent: [path: string, offset: bigint, target: ExtentSeekTarget];
    seekFileExtentById: [fileId: Uint8Array, offset: bigint, target: ExtentSeekTarget];
    setAttributes: [path: string, canonicalBytes: Uint8Array, logicalBytes: bigint | undefined];
    setAttributesById: [fileId: Uint8Array, canonicalBytes: Uint8Array, logicalBytes: bigint | undefined];
    writeNamedAttribute: [path: string, attributeClass: NamedAttributeClass, name: Uint8Array, bytes: Uint8Array, mode: NamedAttributeWriteMode];
  }
>;

export type WasmRawFs = WasmTypedClass<WasmBinding.BrowserFs, {
  createSpeculation: [volumeId: Uint8Array, generationId: Uint8Array, options: SpeculationOptions];
  createVolume: [options: VolumeOptions];
  createVolumeWithId: [volumeId: Uint8Array, options: VolumeOptions];
  exportGenerationBatch: [manifest: WasmImportManifest, cursor: bigint, maximumObjects: number, maximumObjectBytes: bigint];
  importGenerationBatch: [manifest: WasmImportManifest, cursor: bigint, objects: readonly Uint8Array[], maximumObjects: number];
  restoreVolume: [manifest: WasmImportManifest, operationId: Uint8Array];
}, {
  createWorkspace: WasmRawWorkspace;
  openWorkspace: WasmRawWorkspace;
  createVolume: WasmRawVolume;
  createVolumeWithId: WasmRawVolume;
  openVolume: WasmRawVolume;
  restoreVolume: WasmRawVolume;
}, {
  createSpeculation: WasmRawSpeculation;
  objectCacheStats: WasmRawObjectCacheStats;
}, {
  readonly capabilities: EngineCapabilities;
}>;

export type WasmRawResolvedFile = WasmBinding.BrowserResolvedFile;

export type WasmRawResolvedFiles = WasmBinding.BrowserResolvedFiles;

type NativeBoundary<T> =
  T extends Uint8Array ? Uint8Array :
  T extends Promise<infer Value> ? Promise<NativeBoundary<Value>> :
  T extends readonly (infer Value)[] ? readonly NativeBoundary<Value>[] :
  T extends (...args: infer Args) => infer Result
    ? ((...args: { [Key in keyof Args]: NativeBoundary<Args[Key]> }) => NativeBoundary<Result>) &
      { [Key in keyof T]: NativeBoundary<T[Key]> } :
  T extends object ? { [Key in keyof T]: NativeBoundary<T[Key]> } :
  T;

export type NativeBindings = NativeBoundary<typeof NativeBinding>;

export type NativeRawWorkspaceLineageRecord = NativeBoundary<NativeBinding.NativeWorkspaceLineageRecord>;

export type NativeRawWorkspaceGraph = NativeBoundary<NativeBinding.NativeWorkspaceGraph>;

export type NativeRawWatchBatch = NativeBoundary<NativeBinding.NativeWatchBatch>;

export type NativeRawWatcher = NativeBoundary<NativeBinding.NativeWatcher>;

export type NativeRawCheckout = NativeBoundary<NativeBinding.NativeCheckout>;

export type NativeRawTransactionOperation = NativeBoundary<NativeBinding.NativeTransactionOperation>;

export type NativeRawFs = NativeBoundary<NativeBinding.NativeFs>;

export type NativeRawWorkspace = NativeBoundary<NativeBinding.NativeWorkspace>;

export type NativeRawJoinPlan = NativeBoundary<NativeBinding.NativeJoinPlan>;

export type NativeRawSourceResult = NativeBoundary<NativeBinding.NativeSourceResult>;
