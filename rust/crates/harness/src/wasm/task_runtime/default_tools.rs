//! Thin browser types over the same configured recursive source as native.
use super::*;
pub(super) use crate::filesystem::{
    ConfiguredRecursiveToolsConfig as BrowserToolsConfig,
    ConfiguredRecursiveToolSource as BrowserToolSource,
};

#[wasm_bindgen(typescript_custom_section)]
const BROWSER_TOOL_CONFIG_TYPES: &str = r#"
export interface WasmBrowserToolAuthority {
    readonly authority: Authority;
    readonly issuer: string;
    readonly key: readonly number[];
    /** Opaque issuer-authenticated scope returned by issueScopeForAgent. */
    readonly scope: unknown;
}
export interface WasmRecursiveToolsConfig {
    readonly callers: readonly { readonly task: string; readonly owner: WasmBrowserToolAuthority; readonly project: WasmVolumeRefWire; readonly private_volume: WasmVolumeRefWire; readonly result_volume?: WasmVolumeRefWire | null; readonly native_snapshot?: unknown | null }[];
    readonly forks: readonly { readonly caller: string; readonly child: WasmBrowserToolAuthority; readonly project: WasmVolumeRefWire; readonly private_volume: WasmVolumeRefWire; readonly result_volume?: WasmVolumeRefWire | null; readonly admission: WasmTaskAdmissionWire; readonly child_message_pointer: string; readonly child_message_file_pointer?: string | null; readonly input_files?: readonly WasmFileRefWire[]; readonly limits: WasmForkPreparationLimits }[];
    /** Actual coordinator graph root; omission denies foreign project approval. */
    readonly project_policy_graph_root?: string | null;
    readonly merges: readonly { readonly caller: string; readonly source: string; readonly notice: unknown }[];
    readonly stream_provider: WasmProviderRefWire;
    readonly history_limits: HistoryReadLimits;
    readonly inherited_messages: bigint;
    readonly inherited_references: number;
    readonly content_budget: WasmBrowserContentBudget;
}
export interface WasmBrowserContentBudget {
    readonly authorityRecordsRead: bigint; readonly authorityRecordsAppended: bigint;
    readonly authorityBytesRead: bigint; readonly authorityBytesWritten: bigint;
    readonly objectProbes: bigint; readonly backendReadOperations: bigint; readonly backendWriteOperations: bigint;
    readonly durabilityOperations: bigint; readonly pageReads: bigint; readonly pageWrites: bigint;
    readonly objectBytesRead: bigint; readonly objectBytesWritten: bigint;
    readonly bytesHashed: bigint; readonly bytesCopied: bigint; readonly bytesEncoded: bigint;
    readonly sourceBytesRead: bigint; readonly sourcePathComponents: bigint; readonly sourceEntriesVisited: bigint;
    readonly outputBytes: bigint; readonly itemsExamined: bigint; readonly itemsReturned: bigint;
    readonly allocationOperations: bigint; readonly peakAllocationBytes: bigint; readonly materializations: bigint;
}
"#;

#[cfg(all(test, target_arch = "wasm32"))]
mod tests;
