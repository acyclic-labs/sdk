use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use std::mem::size_of;

fn overflow() -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "context clone storage overflows",
    }
}

pub(crate) fn native_vector<T>(
    capacity: usize,
    work: &NativeMaterializationWork,
) -> Result<(Vec<T>, NativeAllocation)> {
    let bytes = capacity.checked_mul(size_of::<T>()).ok_or_else(overflow)? as u64;
    let mut guard = work
        .claim(bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    if bytes != 0 {
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|cause| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause),
            custody: None,
        })?;
    guard
        .resize_retained(
            values
                .capacity()
                .checked_mul(size_of::<T>())
                .ok_or_else(overflow)? as u64,
        )
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    Ok((values, guard))
}

pub(crate) fn clone_retention_native(
    retention: &CompactionRetention,
    work: &NativeMaterializationWork,
) -> Result<(CompactionRetention, NativeAllocation)> {
    let copied = retention
        .roles
        .len()
        .checked_mul(size_of::<crate::model::ModelRole>())
        .ok_or_else(overflow)? as u64;
    work.admit(WorkCounters {
        bytes_copied: copied,
        allocation_operations: u64::from(!retention.roles.is_empty()),
        ..WorkCounters::default()
    })
    .map_err(|cause| Error::NativeWork {
        cause,
        custody: None,
    })?;
    let mut roles = native_vector::<crate::model::ModelRole>(retention.roles.len(), work)?;
    work.charge(WorkCounters {
        bytes_copied: copied,
        ..WorkCounters::default()
    })
    .map_err(|cause| Error::NativeWork {
        cause,
        custody: None,
    })?;
    roles.0.extend_from_slice(&retention.roles);
    Ok((
        CompactionRetention {
            roles: roles.0,
            native_media: retention.native_media,
        },
        roles.1,
    ))
}
