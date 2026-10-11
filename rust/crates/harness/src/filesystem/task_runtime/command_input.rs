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
            additional_capacity_claimed: 0,
            #[cfg(not(target_arch = "wasm32"))]
            additional_custody: Vec::new(),
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        }
    }

    pub(super) fn push_result(&mut self, operation: OperationId, progress: TaskCommandProgress) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        self.prepare_custody_transfer(&progress)?;
        let value = match progress {
            TaskCommandProgress::Ready(value) => value,
            #[cfg(not(target_arch = "wasm32"))]
            TaskCommandProgress::ReadyWithNativeCustody { value, custody: mut incoming } => {
                let Self::Ready { custody, additional_custody, .. } = self else { unreachable!() };
                match custody {
                    None => *custody = Some(incoming),
                    Some(primary) if primary.shares_ledger(&incoming) => {
                        primary.absorb(&mut incoming).map_err(native_error)?;
                    }
                    Some(_) => {
                        if let Some(existing) = additional_custody.iter_mut().find(|guard| guard.shares_ledger(&incoming)) {
                            existing.absorb(&mut incoming).map_err(native_error)?;
                        } else {
                            additional_custody.push(incoming);
                        }
                    }
                }
                value
            }
            TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => unreachable!(),
        };
        let mut envelope = Map::new();
        #[cfg(not(target_arch = "wasm32"))]
        let native = match self {
            Self::Ready { custody: Some(guard), .. } => {
                claim_slots(guard, 2)?;
                let mut uuid = [0; 36];
                let operation = uuid::Uuid::from_bytes(operation.into_bytes()).hyphenated().encode_lower(&mut uuid);
                Some((native_text(guard, "operation_id")?, native_text(guard, operation)?, native_text(guard, "value")?))
            }
            _ => None,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let (operation_key, operation_text, value_key) = native.unwrap_or_else(|| ("operation_id".to_owned(), operation.to_string(), "value".to_owned()));
        #[cfg(target_arch = "wasm32")]
        let (operation_key, operation_text, value_key) = ("operation_id".to_owned(), operation.to_string(), "value".to_owned());
        envelope.insert(operation_key, Value::String(operation_text));
        envelope.insert(value_key, value);
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Ready { value: Value::Array(values), array_capacity_claimed, custody: Some(guard), .. } = self {
            reserve_native_vec(values, array_capacity_claimed, guard)?;
        }
        let Self::Ready { value: Value::Array(values), .. } = self else { unreachable!() };
        values.push(Value::Object(envelope));
        Ok(())
    }

    pub(super) fn input_within_limit(&self, maximum_bytes: u64) -> Result<bool> {
        let Self::Ready { value, .. } = self else { unreachable!() };
        let envelope = Commands { commands: value };
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Ready { custody: Some(guard), .. } = self {
            let io = super::super::fork_preparer::bounded_io::PreparationIo::from_native_work(guard.work())?;
            let bytes = io.encode_json(&envelope, acyclic_stream::MAX_RECORD_BYTES as u64, true)?;
            let within = bytes.len() as u64 <= maximum_bytes;
            drop(bytes);
            return Ok(within);
        }
        let bytes = crate::contract::canonical_json_bytes_bounded(&envelope,
            crate::contract::JsonBounds { maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64, maximum_depth: 256 },
            None, &mut |_| Ok(()))?;
        Ok(bytes.len() as u64 <= maximum_bytes)
    }

    pub(super) fn finish_results(&mut self) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let key = match self {
            Self::Ready { custody: Some(guard), .. } => {
                claim_slots(guard, 1)?;
                native_text(guard, "commands")?
            }
            _ => "commands".to_owned(),
        };
        #[cfg(target_arch = "wasm32")]
        let key = "commands".to_owned();
        let Self::Ready { value, .. } = self else { unreachable!() };
        let mut object = Map::new();
        object.insert(key, std::mem::take(value));
        *value = Value::Object(object);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_custody_transfer(&mut self, progress: &TaskCommandProgress) -> Result<()> {
        let TaskCommandProgress::ReadyWithNativeCustody { custody: incoming, .. } = progress else { return Ok(()); };
        let Self::Ready { custody: Some(primary), additional_custody, additional_capacity_claimed, .. } = self else { return Ok(()); };
        if primary.shares_ledger(incoming) {
            primary.bytes().checked_add(incoming.bytes()).ok_or_else(overflow)?;
        } else if let Some(existing) = additional_custody.iter().find(|guard| guard.shares_ledger(incoming)) {
            existing.bytes().checked_add(incoming.bytes()).ok_or_else(overflow)?;
        } else {
            reserve_native_vec(additional_custody, additional_capacity_claimed, primary)?;
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

#[cfg(not(target_arch = "wasm32"))]
fn overflow() -> Error {
    native_error(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow))
}

#[cfg(not(target_arch = "wasm32"))]
fn claim(guard: &mut acyclic_fs::NativeAllocation, bytes: u64) -> Result<()> {
    guard.resize_retained(guard.bytes().checked_add(bytes).ok_or_else(overflow)?).map_err(native_error)
}

#[cfg(not(target_arch = "wasm32"))]
fn claim_slots(guard: &mut acyclic_fs::NativeAllocation, slots: u64) -> Result<()> {
    claim(guard, slots.checked_mul(std::mem::size_of::<(String, Value)>() as u64).ok_or_else(overflow)?)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_text(guard: &mut acyclic_fs::NativeAllocation, source: &str) -> Result<String> {
    let bytes = source.len() as u64;
    guard.work().admit(acyclic_fs::WorkCounters { bytes_copied: bytes, ..Default::default() }).map_err(native_error)?;
    claim(guard, bytes)?;
    guard.work().charge(acyclic_fs::WorkCounters {
        allocation_operations: u64::from(bytes != 0), ..Default::default()
    }).map_err(native_error)?;
    let mut text = String::new();
    text.try_reserve_exact(source.len()).map_err(|cause| native_error(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause)))?;
    guard.work().charge(acyclic_fs::WorkCounters { bytes_copied: bytes, ..Default::default() }).map_err(native_error)?;
    text.push_str(source);
    Ok(text)
}

#[cfg(not(target_arch = "wasm32"))]
fn reserve_native_vec<T>(values: &mut Vec<T>, claimed: &mut u64, guard: &mut acyclic_fs::NativeAllocation) -> Result<()> {
    if values.len() < values.capacity() { return Ok(()); }
    let required = values.len().checked_add(1).ok_or_else(overflow)?;
    let bytes = required.checked_mul(std::mem::size_of::<T>()).ok_or_else(overflow)? as u64;
    claim(guard, bytes)?;
    guard.work().charge(acyclic_fs::WorkCounters { allocation_operations: 1, ..Default::default() }).map_err(native_error)?;
    if let Err(cause) = values.try_reserve_exact(1) {
        guard.substitute(guard.bytes() - bytes).map_err(native_error)?;
        return Err(native_error(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause)));
    }
    guard.substitute(guard.bytes() - *claimed).map_err(native_error)?;
    *claimed = (values.capacity() * std::mem::size_of::<T>()) as u64;
    Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

    fn finite_budget() -> WorkBudget {
        let mut value = serde_json::to_value(WorkBudget::UNBOUNDED).expect("existing budget fields");
        for field in value.as_object_mut().expect("budget object").values_mut() {
            *field = Value::from(1_000_000_u64);
        }
        serde_json::from_value(value).expect("finite budget")
    }

    fn result(work: &NativeMaterializationWork, text: &str) -> TaskCommandProgress {
        let mut custody = work.retain_owned(0).expect("same-ledger custody");
        let value = Value::String(native_text(&mut custody, text).expect("admitted actual string"));
        TaskCommandProgress::ReadyWithNativeCustody { value, custody }
    }

    #[test]
    fn native_command_envelope_moves_values_and_retains_every_original_ledger() -> Result<()> {
        let first = NativeMaterializationWork::new(finite_budget(), &CancellationToken::new()).expect("first ledger");
        let second = NativeMaterializationWork::new(finite_budget(), &CancellationToken::new()).expect("second ledger");
        let first_baseline = first.live_allocation_bytes();
        let second_baseline = second.live_allocation_bytes();
        let mut ready = CommandDispatch::empty_results();
        for (ordinal, work, text) in [(1, &first, "first"), (2, &second, "second"), (3, &second, "third")] {
            ready.push_result(OperationId::from_bytes([ordinal; 16]), result(work, text))?;
        }
        let CommandDispatch::Ready { additional_custody, .. } = &ready else { unreachable!() };
        assert_eq!(additional_custody.len(), 1, "same additional ledger absorbs without another slot");
        let before = first.live_allocation_bytes();
        assert!(ready.input_within_limit(65_536)?);
        assert_eq!(first.live_allocation_bytes(), before, "temporary encoded envelope is fully retired");
        ready.finish_results()?;
        let CommandDispatch::Ready { value, .. } = &mut ready else { unreachable!() };
        let input = std::mem::take(value);
        assert_eq!(input["commands"][1]["value"], "second");
        assert_eq!(input["commands"][2]["operation_id"], OperationId::from_bytes([3; 16]).to_string());
        assert!(first.live_allocation_bytes() > first_baseline);
        assert!(second.live_allocation_bytes() > second_baseline);
        drop(input);
        drop(ready);
        assert_eq!(first.live_allocation_bytes(), first_baseline);
        assert_eq!(second.live_allocation_bytes(), second_baseline);
        Ok(())
    }

    #[test]
    fn native_command_metadata_copy_denial_precedes_string_allocation() {
        let mut budget = finite_budget();
        budget.bytes_copied = 0;
        let work = NativeMaterializationWork::new(budget, &CancellationToken::new()).expect("state allocation only");
        let baseline = work.live_allocation_bytes();
        let progress = TaskCommandProgress::ReadyWithNativeCustody {
            value: Value::Null, custody: work.retain_owned(0).expect("zero-byte genuine custody"),
        };
        let mut ready = CommandDispatch::empty_results();
        let error = ready.push_result(OperationId::from_bytes([1; 16]), progress).err().expect("copy quota");
        assert!(error.is_native_failure());
        assert_eq!(work.receipt().allocation_operations, 1, "only the actual original state allocation");
        assert_eq!(work.receipt().bytes_copied, 0);
        drop(error);
        drop(ready);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
}
