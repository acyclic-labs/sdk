//! Deliberate variants at the real component's return/transition boundaries.
use super::super::{FileClientError, Pending, PendingRequests, Result};
use crate::{Error, task_volume::FileKey};
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Fault {
    LeakRefund,
    StaleIdentity,
    DoubleCharge,
    SortByIdentity,
    MutationOnRejection,
    WrongKey,
    WrongPayload,
    WrongCursor,
    DropPublication,
    DropChannel,
    DropRequester,
    WrongCommands,
    WrongByteLimit,
    FalseRejection,
    WrongQuotaLimit,
    WrongConflict,
    AllowOverQuota,
    RefusePreflight,
}
thread_local! { static FAULT: Cell<Option<Fault>> = const { Cell::new(None) }; }
pub(super) struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        FAULT.with(|fault| fault.set(None));
    }
}
pub(super) fn select(fault: Option<Fault>) -> Reset {
    FAULT.with(|current| current.set(fault));
    Reset
}

pub(in super::super) fn checked(result: Result<()>) -> Result<()> {
    match FAULT.with(Cell::get) {
        Some(Fault::AllowOverQuota) if result.is_err() => Ok(()),
        Some(Fault::RefusePreflight) if result.is_ok() => Err(FileClientError::ByteQuota {
            limit: super::BYTES,
        }),
        _ => result,
    }
}

pub(in super::super) fn admitted(owner: &mut PendingRequests, result: Result<()>) -> Result<()> {
    match FAULT.with(Cell::get) {
        Some(Fault::DoubleCharge) if result.is_ok() => {
            owner.bytes += owner.entries.back().unwrap().write.bytes.len() as u64;
        }
        Some(Fault::SortByIdentity) if result.is_ok() => {
            owner
                .entries
                .make_contiguous()
                .sort_by(|left, right| left.write.key.cmp(&right.write.key));
        }
        Some(Fault::MutationOnRejection) if result.is_err() => {
            owner.retire_front();
        }
        Some(Fault::WrongCommands) => owner.commands += 1,
        Some(Fault::WrongByteLimit) => owner.limit += 1,
        Some(Fault::FalseRejection) if result.is_ok() => {
            return Err(Error::Conflict("spurious rejection after admission".into()).into());
        }
        Some(Fault::WrongConflict)
            if matches!(result, Err(FileClientError::Harness(Error::Conflict(_)))) =>
        {
            return Err(FileClientError::CommandQuota {
                limit: super::COMMANDS,
            });
        }
        Some(Fault::WrongQuotaLimit) => {
            return match result {
                Err(FileClientError::CommandQuota { limit }) => {
                    Err(FileClientError::CommandQuota { limit: limit + 1 })
                }
                Err(FileClientError::ByteQuota { limit }) => {
                    Err(FileClientError::ByteQuota { limit: limit + 1 })
                }
                other => other,
            };
        }
        _ => (),
    }
    result
}

pub(in super::super) fn retired(owner: &mut PendingRequests, mut done: Pending) -> Pending {
    match FAULT.with(Cell::get) {
        Some(Fault::LeakRefund) => owner.bytes += done.write.bytes.len() as u64,
        Some(Fault::StaleIdentity) => {
            owner.keys.insert(done.write.key.clone());
        }
        Some(Fault::WrongKey) => {
            let path = done.write.key.path().replace("task0/", "other/");
            done.write.key = FileKey::parse(&path).unwrap();
        }
        Some(Fault::WrongPayload) if !done.write.bytes.is_empty() => {
            let mut bytes = done.write.bytes.to_vec();
            bytes[0] ^= 255;
            done.write.bytes = bytes.into();
        }
        Some(Fault::WrongCursor) => done.cursor.volume[0] ^= 255,
        Some(Fault::DropPublication) => done.write.publication = None,
        Some(Fault::DropChannel) => done.write.channel = None,
        Some(Fault::DropRequester) => done.write.requester = None,
        _ => (),
    }
    done
}
