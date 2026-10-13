//! Machine input envelopes retain original command-result custody through consumption.

use super::{CommandDispatch, TaskCommandProgress};
use crate::{Error, OperationId, Result};
use serde::Serialize;
use serde_json::{Map, Value};

#[derive(Serialize)]
struct Commands<'a> {
    commands: &'a Value,
}

impl CommandDispatch {
    pub(super) fn empty_results() -> Self {
        Self::Ready {
            value: Value::Array(Vec::new()),
            #[cfg(not(target_arch = "wasm32"))]
            array_capacity_claimed: 0,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        }
    }

    pub(super) fn push_result(
        &mut self,
        operation: OperationId,
        progress: TaskCommandProgress,
    ) -> Result<()> {
        let value = match progress {
            TaskCommandProgress::Ready(value) => value,
            #[cfg(not(target_arch = "wasm32"))]
            TaskCommandProgress::ReadyWithNativeCustody {
                value,
                custody: mut incoming,
            } => {
                let Self::Ready { custody, .. } = self else {
                    unreachable!()
                };
                match custody {
                    None => *custody = Some(incoming),
                    Some(primary) => {
                        primary.absorb(&mut incoming).map_err(Error::from)?;
                    }
                }
                value
            }
            TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => unreachable!(),
        };
        let mut envelope = Map::new();
        #[cfg(not(target_arch = "wasm32"))]
        let native = match self {
            Self::Ready {
                custody: Some(guard),
                ..
            } => {
                claim_slots(guard, 2)?;
                let mut uuid = [0; 36];
                let operation = uuid::Uuid::from_bytes(operation.into_bytes())
                    .hyphenated()
                    .encode_lower(&mut uuid);
                Some((
                    native_text(guard, "operation_id")?,
                    native_text(guard, operation)?,
                    native_text(guard, "value")?,
                ))
            }
            _ => None,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let (operation_key, operation_text, value_key) = native.unwrap_or_else(|| {
            (
                "operation_id".to_owned(),
                operation.to_string(),
                "value".to_owned(),
            )
        });
        #[cfg(target_arch = "wasm32")]
        let (operation_key, operation_text, value_key) = (
            "operation_id".to_owned(),
            operation.to_string(),
            "value".to_owned(),
        );
        envelope.insert(operation_key, Value::String(operation_text));
        envelope.insert(value_key, value);
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Ready {
            value: Value::Array(values),
            array_capacity_claimed,
            custody: Some(guard),
            ..
        } = self
        {
            reserve_results(values, array_capacity_claimed, guard)?;
        }
        let Self::Ready {
            value: Value::Array(values),
            ..
        } = self
        else {
            unreachable!()
        };
        values.push(Value::Object(envelope));
        Ok(())
    }

    pub(super) fn input_within_limit(&self, maximum_bytes: u64) -> Result<bool> {
        let Self::Ready { value, .. } = self else {
            unreachable!()
        };
        let envelope = Commands { commands: value };
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Ready {
            custody: Some(guard),
            ..
        } = self
        {
            let io = super::super::fork_preparer::bounded_io::PreparationIo::from_native_work(
                guard.work(),
            )?;
            let bytes = io.encode(&envelope, acyclic_stream::MAX_RECORD_BYTES as u64, true)?;
            let within = bytes.len() as u64 <= maximum_bytes;
            drop(bytes);
            return Ok(within);
        }
        let bytes = crate::contract::canonical_json_bytes_bounded(
            &envelope,
            crate::contract::JsonBounds {
                maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64,
                maximum_depth: 256,
            },
            None,
            &mut |_| Ok(()),
        )?;
        Ok(bytes.len() as u64 <= maximum_bytes)
    }

    pub(super) fn finish_results(&mut self) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let key = match self {
            Self::Ready {
                custody: Some(guard),
                ..
            } => {
                claim_slots(guard, 1)?;
                native_text(guard, "commands")?
            }
            _ => "commands".to_owned(),
        };
        #[cfg(target_arch = "wasm32")]
        let key = "commands".to_owned();
        let Self::Ready { value, .. } = self else {
            unreachable!()
        };
        let mut object = Map::new();
        object.insert(key, std::mem::take(value));
        *value = Value::Object(object);
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn overflow() -> Error {
    Error::from(acyclic_fs::NativeTreeMaterializationError::Work(
        acyclic_fs::WorkError::Overflow,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
fn claim(guard: &mut acyclic_fs::NativeAllocation, bytes: u64) -> Result<()> {
    guard
        .resize(guard.bytes().checked_add(bytes).ok_or_else(overflow)?)
        .map_err(Error::from)
}

#[cfg(not(target_arch = "wasm32"))]
fn claim_slots(guard: &mut acyclic_fs::NativeAllocation, slots: u64) -> Result<()> {
    claim(
        guard,
        slots
            .checked_mul(std::mem::size_of::<(String, Value)>() as u64)
            .ok_or_else(overflow)?,
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn native_text(guard: &mut acyclic_fs::NativeAllocation, source: &str) -> Result<String> {
    let bytes = source.len() as u64;
    guard
        .work()
        .admit(acyclic_fs::WorkCounters {
            bytes_copied: bytes,
            ..Default::default()
        })
        .map_err(Error::from)?;
    claim(guard, bytes)?;
    guard
        .work()
        .charge(acyclic_fs::WorkCounters {
            allocation_operations: u64::from(bytes != 0),
            ..Default::default()
        })
        .map_err(Error::from)?;
    let mut text = String::new();
    text.try_reserve_exact(source.len()).map_err(|cause| {
        Error::from(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause))
    })?;
    guard
        .work()
        .charge(acyclic_fs::WorkCounters {
            bytes_copied: bytes,
            ..Default::default()
        })
        .map_err(Error::from)?;
    text.push_str(source);
    Ok(text)
}

#[cfg(not(target_arch = "wasm32"))]
fn reserve_results(
    values: &mut Vec<Value>,
    claimed: &mut u64,
    guard: &mut acyclic_fs::NativeAllocation,
) -> Result<()> {
    if values.len() < values.capacity() {
        return Ok(());
    }
    let required = values.len().checked_add(1).ok_or_else(overflow)?;
    let bytes = required
        .checked_mul(std::mem::size_of::<Value>())
        .ok_or_else(overflow)? as u64;
    claim(guard, bytes)?;
    guard
        .work()
        .charge(acyclic_fs::WorkCounters {
            allocation_operations: 1,
            ..Default::default()
        })
        .map_err(Error::from)?;
    if let Err(cause) = values.try_reserve_exact(1) {
        guard.resize(guard.bytes() - bytes).map_err(Error::from)?;
        return Err(Error::from(
            acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause),
        ));
    }
    guard
        .resize(guard.bytes() - *claimed)
        .map_err(Error::from)?;
    *claimed = (values.capacity() * std::mem::size_of::<Value>()) as u64;
    Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

    fn finite_budget() -> WorkBudget {
        let mut value =
            serde_json::to_value(WorkBudget::UNBOUNDED).expect("existing budget fields");
        for field in value.as_object_mut().expect("budget object").values_mut() {
            *field = Value::from(1_000_000_u64);
        }
        serde_json::from_value(value).expect("finite budget")
    }

    #[test]
    fn native_command_metadata_copy_denial_precedes_string_allocation() {
        let mut budget = finite_budget();
        budget.bytes_copied = 0;
        let work = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .expect("state allocation only");
        let baseline = work.live_allocation_bytes();
        let progress = TaskCommandProgress::ReadyWithNativeCustody {
            value: Value::Null,
            custody: work
                .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
                .expect("zero-byte genuine custody"),
        };
        let mut ready = CommandDispatch::empty_results();
        let error = ready
            .push_result(OperationId::from_bytes([1; 16]), progress)
            .err()
            .expect("copy quota");
        assert!(error.is_native_failure());
        assert_eq!(
            work.receipt().allocation_operations,
            1,
            "only the actual original state allocation"
        );
        assert_eq!(work.receipt().bytes_copied, 0);
        drop(error);
        drop(ready);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
}
