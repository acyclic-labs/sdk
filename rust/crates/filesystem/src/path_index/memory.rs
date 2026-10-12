use super::*;
use std::sync::Mutex;

type Entry = ([u8; 32], Owned<Vec<u8>>);

#[derive(Default)]
pub(crate) struct MemoryGenerationPathIndex {
    entries: Mutex<Option<Owned<Vec<Entry>>>>,
}

fn retained(entries: &Owned<Vec<Entry>>) -> Result<u64, FsError> {
    entries.value.iter().try_fold(entries.bytes, |bytes, (_, value)|
        bytes.checked_add(value.bytes).ok_or_else(|| WorkError::Overflow.into()))
}

fn shifted(count: usize) -> Result<WorkCounters, FsError> {
    let bytes = count.checked_mul(size_of::<Entry>()).ok_or(WorkError::Overflow)?;
    Ok(WorkCounters { bytes_copied: bytes as u64, ..WorkCounters::UNCHARGED })
}

impl MemoryGenerationPathIndex {
    pub(crate) fn load(&self, key: [u8; 32], accounting: &mut KernelAccounting<'_>)
        -> Result<(Option<Vec<u8>>, KernelOwnedAllocation), FsError>
    {
        accounting.check()?;
        let before = snapshot(accounting);
        let result = (|| {
            let Ok(entries) = self.entries.lock() else { return Ok(None); };
            let Some(entries) = entries.as_ref() else { return Ok(None); };
            if before.is_some() { let _ = accounting.claim_owned(retained(entries)?, 0)?; }
            let position = crate::kernel::binary_search_with_accounting(
                &entries.value, accounting, |entry| entry.0.cmp(&key),
            )?;
            position.ok().map(|index| copy(&entries.value[index].1.value, accounting)).transpose()
        })();
        let keep = result.as_ref().ok().and_then(Option::as_ref).map_or(0, |value| value.bytes);
        restore(accounting, before, keep)?;
        match result? {
            Some(owned) => Ok((Some(owned.value), owned.allocation)),
            None => Ok((None, accounting.claim_owned(0, 0)?)),
        }
    }

    pub(crate) fn store(&self, key: [u8; 32], bytes: &[u8], accounting: &mut KernelAccounting<'_>)
        -> Result<(), FsError>
    {
        accounting.check()?;
        if bytes.len() > MAXIMUM_VALUE_BYTES { return Ok(()); }
        let before = snapshot(accounting);
        let result = (|| {
            let Ok(mut state) = self.entries.lock() else { return Ok(()); };
            if before.is_some() && let Some(entries) = state.as_ref() {
                let _ = accounting.claim_owned(retained(entries)?, 0)?;
            }
            if state.is_none() { *state = Some(Owned::<Vec<Entry>>::allocate(MAXIMUM_ENTRIES, accounting)?); }
            let entries = state.as_mut().ok_or(FsError::HandleAllocationFailed)?;
            let incoming = copy(bytes, accounting)?;
            let found = crate::kernel::binary_search_with_accounting(
                &entries.value, accounting, |entry| entry.0.cmp(&key),
            )?;
            if let Ok(index) = found {
                let delta = shifted(entries.value.len() - index - 1)?;
                accounting.admit(delta)?;
                drop(entries.value.remove(index));
                accounting.record_completed(delta)?;
            }
            accounting.charge(WorkCounters { items_examined: entries.value.len() as u64, ..WorkCounters::UNCHARGED })?;
            let mut total = entries.value.iter().map(|(_, value)| value.value.len()).sum::<usize>();
            while entries.value.len() >= MAXIMUM_ENTRIES || total.saturating_add(bytes.len()) > MAXIMUM_RETAINED_BYTES {
                let delta = shifted(entries.value.len() - 1)?;
                accounting.admit(delta)?;
                let (_, evicted) = entries.value.remove(0);
                total -= evicted.value.len();
                drop(evicted);
                accounting.record_completed(delta)?;
            }
            let index = crate::kernel::binary_search_with_accounting(
                &entries.value, accounting, |entry| entry.0.cmp(&key),
            )?.unwrap_or_else(|index| index);
            let delta = shifted(entries.value.len() - index)?;
            accounting.admit(delta)?;
            entries.value.insert(index, (key, incoming));
            accounting.record_completed(delta)
        })();
        restore(accounting, before, 0)?;
        result
    }
}
