use super::{
    Arc, AtomicBool, Delivery, DemandError, DemandWork, HostChange, HostChangeWide,
    NativeSourceWatch, Ordering, size_of,
};
use crate::source_watch::HostChangeSink;
use std::os::windows::io::{AsHandle, AsRawHandle};
use std::sync::atomic::AtomicUsize;
use windows::Win32::Foundation::{FILETIME, HANDLE};
use windows::Win32::Storage::FileSystem::{GetFileTime, SetFileTime};

#[derive(Default)]
struct RenameBatches {
    pairs: AtomicUsize,
    split: AtomicBool,
    birth_changed: AtomicBool,
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
        if changes.iter().any(|change| {
            matches!(change,
                HostChangeWide::Path { action: 3, path, .. }
                    if path.iter().copied().eq("birth".encode_utf16())
            )
        }) {
            self.birth_changed.store(true, Ordering::Release);
        }
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
    let birth = std::fs::File::create(root.path().join("birth"))?;
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
                file: None,
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
    let complete = watch.fence_with_work(&mut work)?;
    let mut creation = FILETIME::default();
    // SAFETY: the original writable file handle and output storage remain live.
    unsafe {
        GetFileTime(
            HANDLE(birth.as_raw_handle()),
            Some(&raw mut creation),
            None,
            None,
        )?
    };
    let ticks = (u64::from(creation.dwHighDateTime) << 32) | u64::from(creation.dwLowDateTime);
    let ticks = ticks.checked_add(10_000_000).ok_or("birth time overflow")?;
    creation.dwLowDateTime = ticks as u32;
    creation.dwHighDateTime = (ticks >> 32) as u32;
    // SAFETY: only creation time changes on the same original writable file.
    unsafe {
        SetFileTime(
            HANDLE(birth.as_raw_handle()),
            Some(&raw const creation),
            None,
            None,
        )?
    };
    drop(birth);
    assert!(watch.fence_with_work(&mut work)?);
    watch.stop_with_work(&mut work)?;
    assert!(
        sink.birth_changed.load(Ordering::Acquire),
        "the default subscription lost a birth-time-only native change"
    );
    assert!(complete);
    assert_eq!(sink.pairs.load(Ordering::Acquire), 1);
    assert!(
        !sink.split.load(Ordering::Acquire),
        "the sink observed half a completed rename"
    );
    Ok(())
}
