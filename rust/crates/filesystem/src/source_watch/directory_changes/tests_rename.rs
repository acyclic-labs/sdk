use super::*;
use crate::source_watch::HostChangeSink;
use std::os::windows::io::AsHandle;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct RenameBatches {
    pairs: AtomicUsize,
    split: AtomicBool,
}

impl HostChangeSink for RenameBatches {
    fn logical_owned_bytes(&self) -> u64 {
        size_of::<Self>() as u64
    }

    fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> {
        work.check()
    }

    fn host_changed_wide(
        &self,
        changes: &[HostChangeWide<'_>],
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        work.check()?;
        let from = changes
            .iter()
            .any(|change| matches!(change, HostChangeWide::Path { action: 4, .. }));
        let to = changes
            .iter()
            .any(|change| matches!(change, HostChangeWide::Path { action: 5, .. }));
        if from && to {
            self.pairs.fetch_add(1, Ordering::AcqRel);
        } else if from || to {
            self.split.store(true, Ordering::Release);
        }
        Ok(())
    }
}

#[test]
fn a_completed_native_rename_reaches_the_sink_in_one_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("before"))?;
    let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
    let sink = Arc::new(RenameBatches::default());
    let cancellation = crate::CancellationToken::new();
    let mut work = DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
    let split_control = RenameBatches::default();
    for (action, name) in [(4, "before"), (5, "after")] {
        let path: Vec<_> = name.encode_utf16().collect();
        split_control.host_changed_wide(
            &[HostChangeWide::Path {
                action,
                path: &path,
            }],
            &mut work,
        )?;
    }
    assert!(split_control.split.load(Ordering::Acquire));
    assert_eq!(split_control.pairs.load(Ordering::Acquire), 0);
    let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
    let watch = NativeSourceWatch::open_watch(
        held.as_handle(),
        delivery,
        false,
        Some(true),
        false,
        &mut work,
    )?;
    std::fs::rename(root.path().join("before"), root.path().join("after"))?;
    let complete = watch.fence_with_work(Duration::from_secs(5), &mut work)?;
    watch.stop_with_work(&mut work)?;
    assert!(complete);
    assert_eq!(sink.pairs.load(Ordering::Acquire), 1);
    assert!(
        !sink.split.load(Ordering::Acquire),
        "the sink observed half a completed rename"
    );
    Ok(())
}
