use super::*;

#[path = "../../../tests/support/creation_budget.rs"]
mod creation_budget;
use creation_budget::READ_ONLY_BUDGET;

/// Recovery retires each attribute page before listing the next, so a retry's
/// peak allocation is bounded by one page of names, not by every name it
/// listed. Names four times longer than the page therefore add at most a
/// couple of pages of peak, never one copy per listed name.
#[tokio::test]
async fn attribute_creation_retry_peak_is_bounded_by_one_page() -> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let operation = IdempotencyKey::from_bytes([0xe7; 16]).operation_id();
    let config = crate::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(crate::access::INITIAL_POLICY),
    };
    let files = [("/f", OwnedBytes::from_static(b"x"))];
    let count = 2_048_usize;
    let short = 8_usize;
    let long = 512_usize;
    let mut peaks = Vec::new();
    for length in [short, long] {
        let attributes = (0..count)
            .map(|index| {
                Ok(crate::CreationAttribute {
                    path: "/f".to_owned(),
                    name: crate::kernel::AttributeName::new(
                        crate::kernel::AttributeClass::PosixXattr,
                        format!("user.{index:04}{}", "x".repeat(length)).into_bytes(),
                    )?,
                    value: OwnedBytes::from_static(b"v"),
                })
            })
            .collect::<Result<Vec<_>, crate::kernel::AttributeError>>()?;
        let creation = || {
            crate::CreationOperation::populated_with_attributes(
                operation,
                &files,
                attributes.clone(),
            )
        };
        let fs = Fs::memory();
        let created = fs
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                "attribute-pages",
                config,
                creation(),
                WorkBudget::UNBOUNDED,
                &cancellation,
                None,
            )
            .await?;
        let retried = fs
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                "attribute-pages",
                config,
                creation(),
                READ_ONLY_BUDGET,
                &cancellation,
                None,
            )
            .await?;
        assert_eq!(retried.value.id(), created.value.id());
        peaks.push(retried.work.peak_allocation_bytes);
    }
    let page = u64::from(crate::facade::ATTRIBUTE_PAGE_ENTRIES);
    let added = (long - short) as u64;
    assert!(
        peaks[1] <= peaks[0] + 2 * page * added,
        "a retry kept every listed attribute name: short names peaked at {}, long names at {}",
        peaks[0],
        peaks[1]
    );
    Ok(())
}
