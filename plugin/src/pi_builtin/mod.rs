//! Pi builtin IO is mediated by the original task owner, not by its cwd.
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_harness::{Error, OperationId, Result, TaskId};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

pub(crate) const PI_BUILTIN_COMMAND_KIND: &str = "acyclic.pi.builtin";
pub(crate) const MAXIMUM_BUILTIN_BYTES: u64 = 262_144;

const BUILTIN_DIAGNOSTIC_LABEL_BYTES: u64 =
    acyclic_harness::preparation_serde_diagnostic_label_bytes() + 187;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PiBuiltinIo {
    Read {
        maximum_bytes: u64,
        #[serde(default)]
        variants: Vec<String>,
    },
    Write {
        content: String,
    },
    EditRead {
        maximum_bytes: u64,
    },
    EditWrite {
        content: String,
        expected_sha256: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiBuiltinRequest {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) path: String,
    pub(crate) io: PiBuiltinIo,
}

impl PiBuiltinRequest {
    fn decode(
        value: &serde_json::Value,
        work: &NativeMaterializationWork,
    ) -> Result<(Self, NativeAllocation)> {
        let encoded =
            crate::pi_root_provider::guarded_pi_bytes(value, MAXIMUM_BUILTIN_BYTES * 2, work)?;
        let decoded = crate::pi_root_provider::guarded_pi_decode::<Self>(
            &encoded.0,
            MAXIMUM_BUILTIN_BYTES * 2,
            MAXIMUM_BUILTIN_BYTES * 4,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            BUILTIN_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        decoded.0.validate()?;
        Ok(decoded)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.sdk_operation.is_empty()
            || self.sdk_operation.len() > 512
            || self.sdk_operation.as_bytes().contains(&0)
        {
            return Err(invalid(
                "builtin requires the original bounded SDK tool call identity",
            ));
        }
        validate_path(&self.path)?;
        match &self.io {
            PiBuiltinIo::Read {
                maximum_bytes,
                variants,
            } => {
                validate_maximum(*maximum_bytes)?;
                if variants.len() > 4 {
                    return Err(invalid("builtin path variant bound exceeded"));
                }
                for path in variants {
                    validate_path(path)?;
                }
            }
            PiBuiltinIo::EditRead { maximum_bytes } => validate_maximum(*maximum_bytes)?,
            PiBuiltinIo::Write { content } => validate_content(content)?,
            PiBuiltinIo::EditWrite {
                content,
                expected_sha256,
            } => {
                validate_content(content)?;
                expected_digest(expected_sha256)?;
            }
        }
        Ok(())
    }

    pub(crate) fn operation(
        &self,
        root: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<OperationId> {
        let original =
            crate::pi_fork::original_operation(root, self.caller, &self.sdk_operation, work)?;
        crate::pi_fork::derived(
            original,
            match self.io {
                PiBuiltinIo::Read { .. } => "builtin-read",
                PiBuiltinIo::Write { .. } => "builtin-write",
                PiBuiltinIo::EditRead { .. } => "builtin-edit-read",
                PiBuiltinIo::EditWrite { .. } => "builtin-edit-write",
            },
            work,
        )
    }

    pub(crate) fn require_root_source_read(&self, original_root: TaskId) -> Result<()> {
        if self.caller != original_root || !matches!(self.io, PiBuiltinIo::Read { .. }) {
            return Err(denied(
                "model builtins cannot mutate the original physical Root source",
            ));
        }
        Ok(())
    }
}

fn validate_maximum(maximum: u64) -> Result<()> {
    if maximum == 0 || maximum > MAXIMUM_BUILTIN_BYTES {
        return Err(invalid(
            "builtin whole-file read exceeds its original byte allowance",
        ));
    }
    Ok(())
}

fn validate_content(content: &str) -> Result<()> {
    if content.len() as u64 > MAXIMUM_BUILTIN_BYTES {
        return Err(invalid("builtin write exceeds its original byte allowance"));
    }
    Ok(())
}

pub(crate) fn expected_digest(value: &str) -> Result<[u8; 32]> {
    let mut digest = [0; 32];
    if value.len() != 64 || hex::decode_to_slice(value, &mut digest).is_err() {
        return Err(invalid(
            "builtin edit requires its exact bounded preimage digest",
        ));
    }
    Ok(digest)
}

pub(crate) fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}

fn denied(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason,
    }
}

type Owner = acyclic_harness::durable_host::TaskJournalOwner<acyclic_stream::LocalStream>;

fn is_absent(error: &Error) -> bool {
    match error {
        Error::NotFound(_) => true,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Io(source),
            ..
        } => source.kind() == std::io::ErrorKind::NotFound,
        Error::NativeView { cause, .. } => matches!(
            cause.as_ref(),
            acyclic_fs::native_mount::MountLifecycleError::Materialization(
                acyclic_fs::NativeTreeMaterializationError::Io(source)
            ) if source.kind() == std::io::ErrorKind::NotFound
        ),
        _ => false,
    }
}

mod native_io;
mod paths;
mod response;
mod root;
mod working_view;
pub(crate) use paths::relative_path;
use paths::{logical_path, scoped_variants, validate_path};
pub(crate) use response::read_response;
pub(crate) use root::authorize_owner;
use working_view::{select_native_path, validate_declared_working_roles};
