use super::*;

/// Enumerate each publisher's create, partial write, file-sync, rename and
/// completed cuts. A rename may be visible before its publisher settles it;
/// the real paired reader must settle both names before returning either.
fn every_pair_cut_is_durable(omit_reader_sync: bool) -> Result<bool, Box<dyn std::error::Error>> {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            publish_probe::kill_at(None);
            publish_probe::omit_reader_sync(false);
        }
    }
    let _reset = Reset;
    let mut safe = true;
    for first in 0..5 {
        for second in 0..5 {
            let directory = tempfile::tempdir()?;
            let root = HostRoot::open(directory.path(), &mut ordinary_work())?;
            for (leaf, cut, byte) in [("first", first, 1), ("second", second, 2)] {
                publish_probe::kill_at((cut < 4).then_some(cut));
                let published =
                    root.read_or_publish(Path::new(leaf), || Ok([byte]), &mut ordinary_work());
                assert_eq!(published.is_ok(), cut == 4);
            }
            publish_probe::omit_reader_sync(omit_reader_sync);
            let cancellation = crate::CancellationToken::new();
            let mut work = DemandWork::new(
                WorkBudget {
                    durability_operations: 1,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            );
            let read = root.read_published_pair::<1, 1, _>(
                Path::new("first"),
                Path::new("second"),
                &mut work,
            )?;
            assert_eq!(read.is_some(), first >= 3 && second >= 3);
            if let Some(records) = read {
                assert_eq!(records, ([1], [2]));
                // A completed second publisher settled both earlier names.
                // Otherwise its renamed entry needs the reader's barrier.
                safe &= second == 4 || work.work.durability_operations == 1;
            }
            publish_probe::omit_reader_sync(false);
        }
    }
    Ok(safe)
}

#[test]
fn every_paired_publication_cut_returns_only_durable_records()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(every_pair_cut_is_durable(false)?);
    Ok(())
}

#[test]
fn negative_control_a_pair_reader_can_outrun_the_publishers_flush()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(!every_pair_cut_is_durable(true)?);
    Ok(())
}

#[test]
fn paired_publications_share_one_namespace_barrier() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = HostRoot::open(directory.path(), &mut ordinary_work())?;
    root.read_or_publish(Path::new("rule"), || Ok(*b"*\n"), &mut ordinary_work())?;
    root.read_or_publish(Path::new("token"), || Ok([7; 16]), &mut ordinary_work())?;
    let budget = WorkBudget {
        durability_operations: 1,
        ..WorkBudget::UNBOUNDED
    };
    let cancellation = crate::CancellationToken::new();
    let mut work = DemandWork::new(budget, &cancellation);
    assert_eq!(
        root.read_published_pair(Path::new("rule"), Path::new("token"), &mut work)?,
        Some((*b"*\n", [7; 16]))
    );
    assert_eq!(work.work.durability_operations, 1);
    work.work.verify(budget)?;

    // The old independent-reader route cannot return both records on the
    // same allowance: it repeats the already settled directory barrier.
    let mut old = DemandWork::new(budget, &cancellation);
    root.read_or_publish(Path::new("rule"), || Ok(*b"*\n"), &mut old)?;
    assert!(
        root.read_or_publish(Path::new("token"), || Ok([7; 16]), &mut old)
            .is_err()
    );
    Ok(())
}

#[test]
fn paired_records_never_escape_a_failed_barrier() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = HostRoot::open(directory.path(), &mut ordinary_work())?;
    root.read_or_publish(Path::new("first"), || Ok([1]), &mut ordinary_work())?;
    root.read_or_publish(Path::new("second"), || Ok([2]), &mut ordinary_work())?;
    let cancellation = crate::CancellationToken::new();
    let mut work = DemandWork::new(
        WorkBudget {
            durability_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    assert!(
        root.read_published_pair::<1, 1, _>(Path::new("first"), Path::new("second"), &mut work)
            .is_err()
    );
    assert_eq!(work.work.durability_operations, 0);
    for (first, second) in [("missing", "second"), ("first", "missing")] {
        assert!(
            root.read_published_pair::<1, 1, _>(
                Path::new(first),
                Path::new(second),
                &mut ordinary_work()
            )?
            .is_none()
        );
    }
    std::fs::write(directory.path().join("second"), [2, 3])?;
    assert!(
        root.read_published_pair::<1, 1, _>(
            Path::new("first"),
            Path::new("second"),
            &mut ordinary_work()
        )
        .is_err()
    );
    Ok(())
}
