use super::*;

pub(crate) fn case_fold_unicode_with_work(
    text: &str,
    work: &mut DemandWork,
) -> Result<Vec<u8>, DemandError> {
    work.reconcile_cancellation_registry()?;
    work.check()?;
    let (allocations, counters, budget, cancellation) = work.allocation_parts();
    let bytes = crate::kernel::types::case_fold_unicode(
        text,
        &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
            allocations,
            counters,
            budget,
            cancellation,
        ),
        cancellation,
    )?
    .0;
    if let Err(error) = work.check() {
        let owned = bytes.capacity() as u64;
        drop(bytes);
        work.release(owned)?;
        return Err(error);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demand_paths_use_the_native_unicode_case_fold() -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        for text in ["ΟΣ", "ος", "ΟΣΑ", "Straße", "ẞ", "ﬃ", "İ"] {
            let name = crate::kernel::LogicalName::new(
                crate::kernel::NameEncoding::Utf8,
                text.as_bytes().to_vec(),
                255,
            )?;
            let folded = case_fold_unicode_with_work(text, &mut work)?;
            assert_eq!(folded, name.case_fold_key(), "{text}");
            let bytes = folded.capacity() as u64;
            drop(folded);
            work.release(bytes)?;
        }
        cancellation.cancel();
        assert!(case_fold_unicode_with_work("ΟΣ", &mut work).is_err());
        Ok(())
    }
}
