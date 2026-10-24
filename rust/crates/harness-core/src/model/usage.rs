//! Shared provider-unit accumulation for live views and canonical turn metadata.

use crate::Result;
use serde_json::Value;

#[doc(hidden)]
pub fn accumulate_usage(total: &mut Value, step: Value) -> Result<()> {
    accumulate_usage_before_insert(total, step, &mut |_| Ok(()))
}

pub(crate) fn accumulate_usage_before_insert(
    total: &mut Value,
    step: Value,
    before_insert: &mut impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    let overflow = || crate::Error::Invalid("usage counter overflow".into());
    match (total, step) {
        // Providers may omit usage on a step; absence cannot erase prior units.
        (_, Value::Null) => {}
        (Value::Object(total), Value::Object(step)) => {
            for (key, value) in step {
                if let Some(previous) = total.get_mut(&key) {
                    accumulate_usage_before_insert(previous, value, before_insert)?;
                } else {
                    before_insert(total.len())?;
                    total.insert(key, value);
                }
            }
        }
        (Value::Number(total), Value::Number(step)) => {
            *total = if let (Some(left), Some(right)) = (total.as_u64(), step.as_u64()) {
                left.checked_add(right).ok_or_else(overflow)?.into()
            } else if let (Some(left), Some(right)) = (total.as_i64(), step.as_i64()) {
                left.checked_add(right).ok_or_else(overflow)?.into()
            } else {
                serde_json::Number::from_f64(
                    total.as_f64().ok_or_else(overflow)? + step.as_f64().ok_or_else(overflow)?,
                )
                .ok_or_else(overflow)?
            };
        }
        (total, step) => *total = step,
    }
    Ok(())
}

// A replay traverses each persisted step once, reconstructing the same
// per-member totals without a process-local usage counter.
pub(crate) fn accumulate_members(
    completed: &mut Value,
    metadata: &mut Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    if let Some(members) = metadata.get_mut("members") {
        let mut previous = completed
            .get_mut("members")
            .map_or(Value::Null, Value::take);
        crate::executor::with_history_io(io, |work| {
            accumulate_usage_before_insert(&mut previous, members.take(), &mut |entries| {
                if let Some(work) = work.as_deref_mut() {
                    // Incoming values retain their original custody;
                    // only the destination B-tree nodes are new work.
                    // A root owns at least one entry and every other
                    // node at least five, including unused slots.
                    if entries.is_multiple_of(5) {
                        work.retain_owned(
                            (11 * std::mem::size_of::<(String, Value)>()
                                + 14 * std::mem::size_of::<usize>())
                                as u64,
                        )?;
                    }
                    let nodes = if entries == 0 {
                        1
                    } else {
                        1 + usize::BITS - entries.leading_zeros()
                    };
                    for _ in 0..nodes {
                        work.allocation_attempt(std::mem::size_of::<(String, Value)>() as u64)?;
                    }
                }
                Ok(())
            })
        })?;
        *members = previous;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn usage_merge_admits_each_new_map_node_before_mutation() {
        let original = json!({"a/tiny":{"tokens":2}});
        let mut total = original.clone();
        let mut insertions = 0;
        assert!(
            accumulate_usage_before_insert(&mut total, json!({"b/tiny":{"tokens":3}}), &mut |_| {
                insertions += 1;
                Err(crate::Error::Invalid("allocation refused".into()))
            })
            .is_err()
        );
        assert_eq!(insertions, 1);
        assert_eq!(total, original);
        // Omitting admission reproduces the old unfunded insertion.
        accumulate_usage(&mut total, json!({"b/tiny":{"tokens":3}})).unwrap();
        assert_ne!(total, original);
        accumulate_usage_before_insert(&mut total, json!({"a/tiny":{"tokens":5}}), &mut |_| {
            panic!("an existing scalar needs no new map node")
        })
        .unwrap();
        assert_eq!(total["a/tiny"]["tokens"], 7);
    }
}
