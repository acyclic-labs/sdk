//! Portable approval and result data for optional native host execution.

use crate::{OperationId, Result, conversation::VolumeRef, resources::GenerationRef};
use acyclic_fs::WorkBudget;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

/// Explicit finite bounds for a dynamically sized native volume view.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeViewOptions {
    /// Absolute owner namespace root, preallocated and witnessed by the host.
    pub root: PathBuf,
    /// Caller-selected positive bound; no fixed volume count is built in.
    pub maximum_volumes: u32,
    /// Per-volume preparation and each later capture/publication work allowance.
    pub work_per_volume: WorkBudget,
}

/// One exact volume exposed to the approved native command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeVolumeMapping {
    /// Exact owner/provider volume reference.
    pub volume: VolumeRef,
    /// Pinned immutable source revision.
    pub generation: GenerationRef,
    /// Actual mechanism for this independently selected volume.
    pub namespace: NativeVolumeNamespace,
    /// Absolute native path, used directly by the command.
    pub path: PathBuf,
    /// Explicit source/destination authority.
    pub writable: bool,
    /// Digest of the verified original volume authority and audience.
    pub authority_revision: [u8; 32],
}

/// Exact native namespace; destination publications remain independent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeViewManifest {
    /// Exact root and finite view bounds.
    pub options: NativeViewOptions,
    /// Ordered dynamically selected volume mappings.
    pub volumes: Vec<NativeVolumeMapping>,
}

/// Namespace semantics for one independently owned volume.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeVolumeNamespace {
    /// Ordinary host directory with its existing SDK publication owner.
    MaterializedDirectory,
    /// Actual existing demand-backed mount.
    LazyMount {
        /// Original exact lazy overlay/shadow and source commitments.
        snapshot: acyclic_fs::LazySnapshotRef,
    },
}

impl NativeViewOptions {
    pub(super) fn budget_for_volume_count(&self, count: usize) -> Result<WorkBudget> {
        use crate::Error;
        let budget = self.work_per_volume;
        if !budget.has_finite_allowances() || count == 0 {
            return Err(Error::Invalid("native work requires finite nonempty allowances".into()));
        }
        let count = u64::try_from(count).map_err(|error| Error::Storage(error.to_string()))?;
        macro_rules! scale {
            ($($field:ident),* $(,)?) => {
                WorkBudget {
                    $($field: budget.$field.checked_mul(count)
                        .ok_or_else(|| Error::Storage("native total allowance overflow".into()))?,)*
                    peak_allocation_bytes: budget.peak_allocation_bytes,
                }
            };
        }
        let scaled = scale!(
            authority_records_read, authority_records_appended, authority_bytes_read,
            authority_bytes_written, object_probes, backend_read_operations,
            backend_write_operations, durability_operations, page_reads, page_writes,
            object_bytes_read, object_bytes_written, bytes_hashed, bytes_copied,
            bytes_encoded, source_bytes_read, source_path_components, source_entries_visited,
            output_bytes, items_examined, items_returned, allocation_operations, materializations,
        );
        if !scaled.has_finite_allowances() {
            return Err(Error::Invalid("native total allowances must remain finite".into()));
        }
        Ok(scaled)
    }
}

impl NativeViewManifest {
    /// One finite validation span for the actual original selection.
    pub fn validation_budget(&self) -> Result<WorkBudget> {
        self.validate()?;
        self.options.budget_for_volume_count(self.volumes.len())
    }

    /// Validate selection shape and finite bounds without opening native paths.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_io(None)
    }

    pub(crate) fn validate_with_io(&self, io: Option<&super::PreparationIo>) -> Result<()> {
        use std::path::Component;

        if !self.options.root.is_absolute()
            || self.options.maximum_volumes == 0
            || self.volumes.is_empty()
            || self.volumes.len() as u64 > u64::from(self.options.maximum_volumes)
            || !self.options.work_per_volume.has_finite_allowances()
            || self
                .options
                .root
                .components()
                .any(|part| matches!(part, Component::ParentDir))
        {
            return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
                "invalid native view selection or bounds"));
        }
        for (index, mapping) in self.volumes.iter().enumerate() {
            if let Some(io) = io {
                io.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
            }
            mapping.volume.validate()?;
            mapping.generation.validate()?;
            if mapping.generation.as_resource().provider() != mapping.volume.provider()
                || !mapping.path.is_absolute()
                || mapping.path == self.options.root
                || !mapping.path.starts_with(&self.options.root)
                || mapping
                    .path
                    .components()
                    .any(|part| matches!(part, Component::ParentDir))
            {
                return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
                    "native volume selection overlaps or escapes its root"));
            }
            for prior in self.volumes.iter().take(index) {
                if let Some(io) = io {
                    io.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
                }
                if mapping.path.starts_with(&prior.path) || prior.path.starts_with(&mapping.path) {
                    return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
                        "native volume selection overlaps or escapes its root"));
                }
            }
        }
        for mapping in &self.volumes {
            if let Some(io) = io {
                io.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
            }
            if let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace
                && (snapshot.workspace_id.into_bytes() == [0; 16]
                    || snapshot.source.identity == [0; 16]
                    || snapshot.authored_generation.digest().as_bytes().as_slice()
                        != mapping.generation.as_resource().key())
            {
                return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
                    "lazy native snapshot differs from its selected generation"));
            }
        }
        Ok(())
    }
}

pub(super) fn validation_error(
    io: Option<&super::PreparationIo>, code: crate::wire::ErrorCode, reason: &'static str,
) -> crate::Error {
    #[cfg(not(target_arch = "wasm32"))]
    if io.and_then(super::PreparationIo::original_native_work).is_some() {
        return crate::Error::NativeBoundary { code, reason };
    }
    let _ = io;
    match code {
        crate::wire::ErrorCode::Unsupported => crate::Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Unauthorized => crate::Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => crate::Error::Conflict(reason.into()),
        _ => crate::Error::Invalid(reason.into()),
    }
}

/// Versioned effect kind supported by the optional one-shot process provider.
pub const NATIVE_PROCESS_EFFECT_KIND: &str = "harness/native-process/v1";

/// Exact approved executable invocation. Host credentials are not request fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeProcessRequest {
    /// Absolute executable path; ambient PATH lookup is never used.
    pub executable: PathBuf,
    /// Ordered arguments, passed directly without implicit shell expansion.
    pub argv: Vec<String>,
    /// Absolute working directory within the selected native view.
    pub cwd: PathBuf,
    /// Complete environment after clearing the inherited host environment.
    pub environment: BTreeMap<String, String>,
    /// Positive finite capture allowance in milliseconds.
    pub timeout_ms: u32,
    /// Positive finite wait for each durable task-intent check.
    pub control_timeout_ms: u32,
    /// Positive interval between durable task-intent checks during capture.
    pub cancellation_poll_ms: u32,
    /// Positive bound across stdout and stderr together.
    pub maximum_output_bytes: u32,
    /// Positive bound for the complete serialized result, including metadata.
    pub maximum_result_bytes: u32,
    /// Optional exact MCP exchange; mutually exclusive with literal standard input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_stdio: Option<crate::mcp::stdio::McpStdioRequest>,
    /// Exact literal bytes written to stdin, followed by EOF without terminating the process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standard_input: Option<Vec<u8>>,
    /// Exact namespace, volumes, generations, authority revisions and work bounds.
    pub view: NativeViewManifest,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl super::PreparationRecord for NativeProcessRequest {
    fn maximum_slot_bytes() -> u64 {
        let typed = [
            std::mem::size_of::<Self>() as u64,
            std::mem::size_of::<NativeVolumeMapping>() as u64,
            std::mem::size_of::<(String, String)>() as u64,
            std::mem::size_of::<serde_json::Value>() as u64,
            std::mem::size_of::<(String, serde_json::Value)>() as u64,
            std::mem::size_of::<crate::mcp::stdio::McpStdioRequest>() as u64,
        ].into_iter().max().unwrap_or(0);
        let content = std::mem::size_of::<serde::__private229::de::Content<'static>>()
            .max(std::mem::size_of::<(
                serde::__private229::de::Content<'static>,
                serde::__private229::de::Content<'static>,
            )>()) as u64;
        typed + 2 * content
    }

    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::NativeProcessRequest
    }
}

impl NativeProcessRequest {
    /// Decodes the pinned approved request with the canonical closed-record codec.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn decode_with_native_work(
        bytes: &[u8],
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let io = super::PreparationIo::from_native_work(work)?;
        io.decode_preparation_with_code::<Self>(
            bytes, crate::wire::ErrorCode::Storage,
        )?.into_native_parts()
    }

    /// Checks only capture/control/result arithmetic, without consulting the OS.
    pub const fn has_valid_capture_allowances(&self) -> bool {
        super::native_capture_bounds::capture_allowances_valid(
            self.timeout_ms,
            self.control_timeout_ms,
            self.cancellation_poll_ms,
            self.maximum_output_bytes,
            self.maximum_result_bytes,
        )
    }

    /// Binds the existing approval mechanism to the task, command and exact request.
    pub fn approval_digest(&self, task: crate::TaskId, command: OperationId) -> Result<[u8; 32]> {
        self.approval_digest_with_io(task, command, None)
    }

    /// Computes the same approval digest using the caller's original finite work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn approval_digest_with_native_work(
        &self, task: crate::TaskId, command: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<[u8; 32]> {
        if !work.remaining().map_err(|cause| crate::Error::NativeWork {
            cause, custody: None,
        })?.has_finite_allowances() {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native approval digest requires finite original work",
            });
        }
        let io = super::PreparationIo::from_native_work(work)?;
        self.approval_digest_with_io(task, command, Some(&io))
    }

    pub(crate) fn approval_digest_with_io(
        &self, task: crate::TaskId, command: OperationId,
        io: Option<&super::PreparationIo>,
    ) -> Result<[u8; 32]> {
        let value = (NATIVE_PROCESS_EFFECT_KIND, task, command, self);
        let Some(io) = io else { return crate::contract::canonical_json_digest(&value); };
        let guard;
        let encoded = io.encode(&value, io.remaining()?.bytes_encoded, true)?;
        guard = io.guard_bytes(&encoded)?;
        io.charge(acyclic_fs::WorkCounters { bytes_hashed: encoded.len() as u64, ..Default::default() })?;
        let digest = *blake3::hash(&encoded).as_bytes();
        drop(encoded);
        drop(guard);
        Ok(digest)
    }

    /// Decodes a pinned complete MCP process receipt, with no process or network I/O.
    pub fn mcp_response(&self, result: &NativeProcessResult) -> Result<serde_json::Value> {
        let exchange = self.mcp_stdio.as_ref().ok_or_else(|| {
            crate::Error::Unsupported("native request has no MCP exchange".into())
        })?;
        if result.stop.is_some() {
            return Err(crate::Error::Indeterminate(exchange.operation));
        }
        exchange.decode_response(&result.stdout)
    }

}

/// Bounded bytes from a completed process. Bytes preserve non-UTF-8 output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeProcessResult {
    /// Whether the OS exit status indicates success; nonzero exits still return their captured output as a known invocation result.
    pub success: bool,
    /// Exit code, or absent when the OS reports termination by signal.
    pub exit_code: Option<i32>,
    /// Bounded stdout bytes.
    pub stdout: Vec<u8>,
    /// Bounded stderr bytes.
    pub stderr: Vec<u8>,
    /// Present for a stopped capture; stdout/stderr then contain bounded prefixes.
    pub stop: Option<NativeProcessStop>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl super::PreparationRecord for NativeProcessResult {
    fn maximum_slot_bytes() -> u64 {
        [
            std::mem::size_of::<Self>(),
            std::mem::size_of::<NativeProcessStop>(),
            std::mem::size_of::<Vec<u8>>(),
            std::mem::size_of::<String>(),
            std::mem::size_of::<(String, u8)>(),
        ].into_iter().max().unwrap_or(0) as u64
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl NativeProcessResult {
    /// Decodes a pinned result with the canonical closed-record decoder.
    pub fn decode_with_native_work(
        bytes: &[u8],
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let io = super::PreparationIo::from_native_work(work)?;
        let mut decoded = io.decode_preparation_with_code::<Self>(
            bytes, crate::wire::ErrorCode::Storage,
        )?;
        let owned = (std::mem::size_of::<Self>() as u64)
            .checked_add(decoded.stdout.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(decoded.stderr.capacity() as u64))
            .and_then(|bytes| bytes.checked_add(
                decoded.stop.as_ref().map_or(0, |stop| stop.message.capacity() as u64),
            ))
            .ok_or(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "native result ownership overflow",
            })?;
        decoded.substitute_owned(owned)?;
        decoded.into_native_parts()
    }
}

/// Why capture stopped, including whether cleanup was confirmed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeProcessStop {
    /// Typed capture condition, independent of diagnostic wording.
    pub kind: NativeProcessStopKind,
    /// Bounded diagnostic from the process owner.
    pub message: String,
    /// Confirmed cleanup within the platform containment assumptions.
    pub cleanup_completed: bool,
}

/// Conditions that stop bounded native output collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeProcessStopKind {
    /// Capture allowance expired.
    Timeout,
    /// Combined stdout/stderr allowance was exhausted.
    OutputLimit,
    /// Durable task intent, ownership, or its observation stopped execution.
    ControlStopped,
    /// Pipe observation or containment cleanup failed.
    CaptureFailed,
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_result_decode_tests {
    use super::*;

    #[test]
    fn closed_request_decode_keeps_mcp_value_and_original_custody() {
        let request = NativeProcessRequest {
            executable: PathBuf::from("native/bin"),
            argv: vec!["argument".into()],
            cwd: PathBuf::from("native"),
            environment: BTreeMap::from([("KEY".into(), "value".into())]),
            timeout_ms: 1,
            control_timeout_ms: 1,
            cancellation_poll_ms: 1,
            maximum_output_bytes: 1,
            maximum_result_bytes: 1,
            mcp_stdio: Some(crate::mcp::stdio::McpStdioRequest {
                initialization: OperationId::from_bytes([1; 16]),
                operation: OperationId::from_bytes([2; 16]),
                method: crate::mcp::stdio::McpStdioMethod::CallTool,
                params: serde_json::json!({"name":"tool","arguments":{"nested":[1,null,{"key":"value"}]}}),
                maximum_bytes: 1024,
            }),
            standard_input: None,
            view: NativeViewManifest {
                options: NativeViewOptions {
                    root: PathBuf::from("native"),
                    maximum_volumes: 1,
                    work_per_volume: WorkBudget::UNBOUNDED,
                },
                volumes: Vec::new(),
            },
        };
        let bytes = crate::contract::canonical_json_bytes(&request).unwrap();
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED, &cancellation,
        ).unwrap();
        let source_custody = work.retain_owned(bytes.capacity() as u64).unwrap();
        let (decoded, custody) = NativeProcessRequest::decode_with_native_work(&bytes, &work).unwrap();
        assert_eq!(decoded, request);
        assert!(custody.bytes() >= std::mem::size_of::<NativeProcessRequest>() as u64);
        assert!(custody.work().shares_ledger(&work));
        drop(decoded);
        drop(custody);
        drop(bytes);
        drop(source_custody);
    }

    #[test]
    fn closed_result_decode_retains_actual_payload_on_original_ledger() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED, &cancellation,
        ).unwrap();
        let bytes = br#"{"success":false,"exit_code":null,"stdout":[255,0],"stderr":[1],"stop":{"kind":"capture_failed","message":"partial capture","cleanup_completed":false}}"#;
        let (result, custody) = NativeProcessResult::decode_with_native_work(bytes, &work).unwrap();
        assert_eq!(result.stdout, [255, 0]);
        assert_eq!(result.stderr, [1]);
        let stop = result.stop.as_ref().unwrap();
        assert_eq!(stop.kind, NativeProcessStopKind::CaptureFailed);
        assert!(!stop.cleanup_completed);
        let actual = std::mem::size_of::<NativeProcessResult>() as u64
            + result.stdout.capacity() as u64
            + result.stderr.capacity() as u64
            + stop.message.capacity() as u64;
        assert_eq!(custody.bytes(), actual);
        assert!(custody.work().shares_ledger(&work));
        drop(result);
        drop(custody);
    }

    #[test]
    fn closed_result_decode_preserves_storage_error_and_paid_diagnostic() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED, &cancellation,
        ).unwrap();
        match NativeProcessResult::decode_with_native_work(b"{}", &work).unwrap_err() {
            crate::Error::NativeDecode { code, source, custody: Some(custody) } => {
                assert_eq!(code, crate::wire::ErrorCode::Storage);
                assert!(custody.work().shares_ledger(&work));
                drop(source);
                drop(custody);
            }
            error => panic!("unexpected decode error: {error:?}"),
        }
    }
}
