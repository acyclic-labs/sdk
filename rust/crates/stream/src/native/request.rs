use super::*;

impl CommitRequest {
    /// Retains an exact request copy on its original native allocation ledger.
    ///
    /// Only control vectors are copied. Paths, indexed headers, retry identity
    /// and record bytes retain their existing immutable owners. Every physical
    /// vector is admitted before allocation and remains funded with the result.
    /// Work is proportional to this bounded request, independent of history.
    pub fn try_clone_native(
        &self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<NativeStreamOutput<Self>> {
        let mut allocation = acyclic_native_runtime::ReadAllocation::empty(meter);
        let mut effects = vector(self.effects.len(), meter, &mut allocation)?;
        for effect in &self.effects {
            let conditions = copy_vector(&effect.conditions, meter, &mut allocation)?;
            let mut mutations = vector(effect.mutations.len(), meter, &mut allocation)?;
            for mutation in &effect.mutations {
                let copied = match mutation {
                    CommitMutation::Append { path, records } => CommitMutation::Append {
                        path: path.clone(),
                        records: copy_vector(records, meter, &mut allocation)?,
                    },
                    CommitMutation::Fork {
                        exclude,
                        source,
                        destination,
                        at_tail,
                        records,
                    } => CommitMutation::Fork {
                        exclude: copy_vector(exclude, meter, &mut allocation)?,
                        source: source.clone(),
                        destination: destination.clone(),
                        at_tail: *at_tail,
                        records: copy_vector(records, meter, &mut allocation)?,
                    },
                };
                push(&mut mutations, copied, meter)?;
            }
            push(
                &mut effects,
                CommitEffect {
                    conditions,
                    mutations,
                    soft: effect.soft,
                },
                meter,
            )?;
        }
        meter.check()?;
        Ok(NativeStreamOutput::new(
            Self {
                deadline_unix_millis: self.deadline_unix_millis,
                effects,
                idempotency_key: self.idempotency_key.clone(),
            },
            allocation,
        ))
    }
}

/// Moves one value into an admitted slot, charging its copy.
fn push<T>(
    output: &mut Vec<T>,
    value: T,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<()> {
    let bytes = std::mem::size_of::<T>() as u64;
    meter.admit_codec(0, bytes, 0)?;
    output.push(value);
    meter.record_codec(0, bytes, 0)
}

fn copy_vector<T: Clone>(
    source: &[T],
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    allocation: &mut acyclic_native_runtime::ReadAllocation,
) -> std::io::Result<Vec<T>> {
    let mut output = vector(source.len(), meter, allocation)?;
    let bytes = std::mem::size_of_val(source) as u64;
    meter.admit_codec(0, bytes, 0)?;
    output.extend_from_slice(source);
    meter.record_codec(0, bytes, 0)?;
    Ok(output)
}

#[cfg(test)]
mod tests;
