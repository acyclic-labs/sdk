//! A burst of changes past a watch's buffer loses none of them, and the watch
//! grows its buffer until a burst of the same size arrives whole.
use super::*;
use crate::source_watch::HostChangeSink;
use std::collections::BTreeSet;
use std::os::windows::io::AsHandle;
use std::sync::atomic::AtomicUsize;

/// Files per burst. Their creation records, about 160 bytes each, overflow
/// the initial 4 KiB but fit the 64 KiB ceiling.
const FILES: usize = 40;
/// Bursts before a growing buffer must have caught up: 4 KiB doubles to
/// 64 KiB in four.
const BURSTS: usize = 6;

#[derive(Default)]
struct Seen {
    names: Mutex<BTreeSet<String>>,
    rescans: AtomicUsize,
}

impl HostChangeSink for Seen {
    fn logical_owned_bytes(&self) -> u64 {
        0
    }

    fn host_changed(
        &self,
        changes: &[HostChange],
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        work.check()?;
        for change in changes {
            if matches!(change, HostChange::Everything) {
                self.rescans.fetch_add(1, Ordering::AcqRel);
            }
        }
        Ok(())
    }

    fn host_changed_wide(
        &self,
        changes: &[HostChangeWide<'_>],
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        work.check()?;
        let mut names = self.names.lock().unwrap_or_else(PoisonError::into_inner);
        for change in changes {
            match change {
                HostChangeWide::Path { path, .. } => {
                    names.insert(String::from_utf16_lossy(path));
                }
                HostChangeWide::Everything => {
                    self.rescans.fetch_add(1, Ordering::AcqRel);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Runs bursts against a watch whose buffer grows to at most `maximum`
/// bytes, and answers the first burst that arrived whole, without a rescan.
fn first_whole_burst(maximum: u64) -> Result<Option<usize>, Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
    let sink = Arc::new(Seen::default());
    let cancellation = crate::CancellationToken::new();
    let mut work = DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
    let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
    let watch = NativeSourceWatch::open_watch_growing_to(
        held.as_handle(),
        delivery,
        false,
        Some(true),
        false,
        maximum,
        &mut work,
    )?;
    let mut whole = None;
    for burst in 0..BURSTS {
        let rescans = sink.rescans.load(Ordering::Acquire);
        let names: BTreeSet<String> = (0..FILES)
            .map(|file| format!("burst-{burst}-file-{file:04}-padded-to-a-longer-name"))
            .collect();
        {
            // Nothing drains while the reads are held, so the whole burst
            // lands in the kernel's buffer before the next read.
            let _held = watch.paused();
            for name in &names {
                std::fs::File::create(root.path().join(name))?;
            }
        }
        assert!(watch.fence_with_work(Duration::from_secs(10), &mut work)?);
        let rescanned = sink.rescans.load(Ordering::Acquire) > rescans;
        let seen = sink.names.lock().unwrap_or_else(PoisonError::into_inner);
        let missing = names.difference(&seen).count();
        // A burst is reported change by change or by a rescan; never lost.
        assert!(
            rescanned || missing == 0,
            "burst {burst} lost {missing} changes without a rescan"
        );
        if !rescanned {
            whole = Some(burst);
            break;
        }
        assert!(
            watch.shared.buffer_bytes.load(Ordering::Acquire) <= maximum.max(INITIAL_BUFFER_BYTES)
        );
    }
    watch.stop_with_work(&mut work)?;
    assert_eq!(watch.shared.buffer_bytes.load(Ordering::Acquire), 0);
    Ok(whole)
}

#[test]
fn a_burst_past_the_buffer_loses_no_change_and_grows_the_buffer()
-> Result<(), Box<dyn std::error::Error>> {
    let whole = first_whole_burst(MAXIMUM_BUFFER_BYTES)?;
    // The first burst overflows 4 KiB; a later one arrives whole.
    assert!(
        matches!(whole, Some(burst) if burst >= 1),
        "first whole burst: {whole:?}"
    );
    // Negative control: a buffer that never grows rescans every burst.
    assert_eq!(first_whole_burst(INITIAL_BUFFER_BYTES)?, None);
    Ok(())
}
