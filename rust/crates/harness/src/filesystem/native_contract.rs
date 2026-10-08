//! Portable approval and result data for optional native host execution.
//! These types perform no OS operations and carry no host credentials.

use crate::{OperationId, Result, conversation::VolumeRef, resources::GenerationRef};
use acyclic_fs::WorkBudget;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

/// Explicit finite bounds for a dynamically sized native volume view.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeViewOptions {
    /// Existing empty absolute directory owned by the embedding host.
    pub root: PathBuf,
    /// Caller-selected positive bound; no fixed volume count is built in.
    pub maximum_volumes: u32,
    /// Per-volume preparation and each later capture/publication work allowance.
    /// Total work scales by the admitted volume count; there is no joint commit.
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
    /// Absolute native path, used directly by the command.
    pub path: PathBuf,
    /// Explicit source/destination authority.
    pub writable: bool,
    /// Digest of the verified original volume authority and audience.
    pub authority_revision: [u8; 32],
}

/// Materialized namespace semantics admitted with a command and its approval.
/// All directories reside on the host filesystem: cross-directory rename follows
/// that filesystem, and destination publications are independent. This is a
/// materialized view, never a claim that an OS mount driver is running.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeViewManifest {
    /// Actual namespace mechanism, bound into admission and approval.
    pub kind: NativeNamespaceKind,
    /// Exact root and finite view bounds.
    pub options: NativeViewOptions,
    /// Ordered dynamically selected volume mappings.
    pub volumes: Vec<NativeVolumeMapping>,
}

/// Namespace semantics implemented by this optional adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeNamespaceKind {
    /// Ordinary host directories with independent SDK destination publication.
    MaterializedDirectories,
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
    /// Absolute working directory within the selected materialized view.
    pub cwd: PathBuf,
    /// Complete environment after clearing the inherited host environment.
    pub environment: BTreeMap<String, String>,
    /// Positive finite capture allowance in milliseconds. Intent checks and
    /// mandatory cleanup have separate admitted/platform observation bounds.
    pub timeout_ms: u32,
    /// Positive finite wait for each durable task-intent check.
    pub control_timeout_ms: u32,
    /// Positive interval between durable task-intent checks during capture.
    pub cancellation_poll_ms: u32,
    /// Positive bound across stdout and stderr together.
    pub maximum_output_bytes: u32,
    /// Positive bound for the complete serialized result, including metadata.
    pub maximum_result_bytes: u32,
    /// Optional exact MCP exchange. Absence preserves the closed-stdin contract
    /// and canonical approval bytes of existing native process requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_stdio: Option<crate::mcp::stdio::McpStdioRequest>,
    /// Exact namespace, volumes, generations, authority revisions and work bounds.
    pub view: NativeViewManifest,
}

impl NativeProcessRequest {
    /// Checks only capture/control/result arithmetic, without consulting the OS.
    /// Native admission additionally validates paths, environment and authority.
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
        crate::contract::canonical_json_digest(&(NATIVE_PROCESS_EFFECT_KIND, task, command, self))
    }

    /// Decodes a pinned complete MCP process receipt, with no process or network
    /// I/O. Intentional protocol completion may terminate the server with a
    /// nonzero exit status; any stopped capture remains uncertain.
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
    /// Whether the OS exit status indicates success; nonzero exits still return
    /// their captured output as a known invocation result.
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
