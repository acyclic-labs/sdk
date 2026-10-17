use super::super::adapter::NativeDemandCarrier;
use super::{MAXIMUM_TRACKED_KEYS, ViewChange, ViewLedger, ViewOrigin, ViewStamp};
use crate::FileId;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use crate::model::VolumeLimits;
use std::sync::{Arc, Mutex};

fn work() -> crate::demand::DemandWork {
    crate::demand::DemandWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
}

fn path(names: &[&str]) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let components = names
        .iter()
        .map(|name| {
            LogicalName::new(
                NameEncoding::Utf8,
                name.as_bytes().to_vec(),
                limits.maximum_component_bytes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NamespacePath::new(components, limits)?)
}

#[test]
fn unrelated_changes_never_invalidate_a_lookup() -> Result<(), Box<dyn std::error::Error>> {
    let mut work = work();
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut work))?;
    let watched = path(&["corpus", "file"])?;
    let file_id = FileId::new();
    let stamp = ledger.stamp();
    {
        let mut carrier = NativeDemandCarrier::Borrowed(&mut work);
        for index in 0..20_000 {
            let created = path(&["writes", &format!("file-{index}")])?;
            ledger.record(&ViewChange::Bound(&created), &mut carrier)?;
            ledger.record(&ViewChange::Node(FileId::new()), &mut carrier)?;
        }
    }
    assert!(ledger.unchanged_since(&watched, Some(file_id), stamp, &mut work)?);
    assert!(ledger.unchanged_since(&path(&["corpus"])?, None, stamp, &mut work)?);
    assert!(!ledger.unchanged_since(&path(&["writes"])?, None, stamp, &mut work)?);
    ledger.record(
        &ViewChange::Node(file_id),
        &mut NativeDemandCarrier::Borrowed(&mut work),
    )?;
    assert!(!ledger.unchanged_since(&watched, Some(file_id), stamp, &mut work)?);
    Ok(())
}

#[test]
fn every_spelling_of_a_name_shares_its_changes() -> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let name = |encoding, text: &str| {
        let bytes = match encoding {
            NameEncoding::WindowsUtf16Le => {
                text.encode_utf16().flat_map(u16::to_le_bytes).collect()
            }
            NameEncoding::Utf8 | NameEncoding::PosixBytes => text.as_bytes().to_vec(),
        };
        LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
    };
    let directory = name(NameEncoding::Utf8, "src")?;
    let under = |name| NamespacePath::new(vec![directory.clone(), name], limits);
    let spellings = [
        (
            name(NameEncoding::Utf8, "Readme.MD")?,
            name(NameEncoding::PosixBytes, "README.md")?,
        ),
        (
            name(NameEncoding::WindowsUtf16Le, "ReadMe.md")?,
            name(NameEncoding::Utf8, "readme.MD")?,
        ),
        (
            name(NameEncoding::PosixBytes, "\u{e9}t\u{e9}")?,
            name(NameEncoding::WindowsUtf16Le, "E\u{301}TE\u{301}")?,
        ),
    ];
    for (changed, looked_up) in spellings {
        let mut work = work();
        let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut work))?;
        let looked_up = under(looked_up)?;
        let stamp = ledger.stamp();
        ledger.record(
            &ViewChange::Bound(&under(changed)?),
            &mut NativeDemandCarrier::Borrowed(&mut work),
        )?;
        assert!(!ledger.unchanged_since(&looked_up, None, stamp, &mut work)?);
    }
    let mut work = work();
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut work))?;
    let other = under(name(NameEncoding::Utf8, "readme.txt")?)?;
    let stamp = ledger.stamp();
    ledger.record(
        &ViewChange::Bound(&under(name(NameEncoding::Utf8, "readme.md")?)?),
        &mut NativeDemandCarrier::Borrowed(&mut work),
    )?;
    assert!(
        ledger.unchanged_since(&other, None, stamp, &mut work)?,
        "distinct names stay distinct"
    );
    let raw = |bytes: &[u8]| {
        LogicalName::new(
            NameEncoding::PosixBytes,
            bytes.to_vec(),
            limits.maximum_component_bytes,
        )
    };
    let looked_up = under(raw(&[0xff, b'a'])?)?;
    let stamp = ledger.stamp();
    ledger.record(
        &ViewChange::Bound(&under(raw(&[0xff, b'A'])?)?),
        &mut NativeDemandCarrier::Borrowed(&mut work),
    )?;
    assert!(ledger.unchanged_since(&looked_up, None, stamp, &mut work)?);
    Ok(())
}

#[test]
fn full_case_fold_aliases_share_native_cache_invalidation() -> Result<(), Box<dyn std::error::Error>>
{
    for (changed, looked_up) in [
        ("ΟΣ", "ος"),
        ("Straße", "STRASSE"),
        ("ß", "ẞ"),
        ("ﬃ", "FFI"),
    ] {
        let mut work = work();
        let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut work))?;
        let stamp = ledger.stamp();
        let looked_up = path(&["src", looked_up])?;
        ledger.record(
            &ViewChange::Bound(&path(&["src", changed])?),
            &mut NativeDemandCarrier::Borrowed(&mut work),
        )?;
        assert!(!ledger.unchanged_since(&looked_up, None, stamp, &mut work)?);
        assert!(!ledger.reported_unchanged_since(&looked_up, None, stamp, &mut work)?);
    }
    Ok(())
}

#[test]
fn folded_keys_invalidate_only_older_lookups() -> Result<(), Box<dyn std::error::Error>> {
    let mut work = work();
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut work))?;
    let watched = path(&["watched"])?;
    let before = ledger.stamp();
    {
        let mut carrier = NativeDemandCarrier::Borrowed(&mut work);
        for _ in 0..=MAXIMUM_TRACKED_KEYS {
            ledger.record(&ViewChange::Node(FileId::new()), &mut carrier)?;
        }
    }
    let after = ledger.stamp();
    let untouched = FileId::new();
    assert!(
        !ledger.unchanged_since(&watched, Some(untouched), before, &mut work)?,
        "a folded map may not vouch for lookups older than what it folded"
    );
    assert!(ledger.unchanged_since(&watched, Some(untouched), after, &mut work)?);
    Ok(())
}

#[test]
fn observers_learn_each_change_with_its_origin() -> Result<(), Box<dyn std::error::Error>> {
    let mut work = work();
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut work))?;
    let recorded = Arc::new(Recorded::default());
    let observer: std::sync::Weak<Recorded> = Arc::downgrade(&recorded);
    ledger.observe(observer, &mut NativeDemandCarrier::Borrowed(&mut work))?;
    let origin = ViewOrigin::new();
    let mut carrier = NativeDemandCarrier::Borrowed(&mut work);
    ledger.record(&ViewChange::Everything, &mut carrier)?;
    {
        let _scope = origin.enter();
        ledger.record(&ViewChange::Everything, &mut carrier)?;
    }
    ledger.record(&ViewChange::Everything, &mut carrier)?;
    let recorded = recorded.0.lock().map_err(|_| "poisoned")?.clone();
    let origins = recorded
        .iter()
        .map(|(_, origin)| *origin)
        .collect::<Vec<_>>();
    assert_eq!(
        origins,
        [ViewOrigin::UNATTRIBUTED, origin, ViewOrigin::UNATTRIBUTED]
    );
    assert!(recorded.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert_eq!(
        recorded.last().map(|(position, _)| *position),
        Some(ledger.stamp())
    );
    Ok(())
}

#[derive(Default)]
struct Recorded(Mutex<Vec<(ViewStamp, ViewOrigin)>>);

impl super::ViewObserver for Recorded {
    fn view_changed(&self, position: ViewStamp, origin: ViewOrigin) {
        if let Ok(mut recorded) = self.0.lock() {
            recorded.push((position, origin));
        }
    }
}

#[test]
fn publication_delivers_to_final_live_observers() -> Result<(), Box<dyn std::error::Error>> {
    let mut setup = work();
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut setup))?;
    let early: Arc<dyn super::ViewObserver> = Arc::new(Recorded::default());
    let dead = Arc::downgrade(&early);
    ledger.observe(dead.clone(), &mut NativeDemandCarrier::Borrowed(&mut setup))?;
    drop(early);
    assert!(dead.upgrade().is_none());
    let counted = Arc::new(Recorded::default());
    let late: Arc<dyn super::ViewObserver> = counted.clone();
    ledger.observe(
        Arc::downgrade(&late),
        &mut NativeDemandCarrier::Borrowed(&mut setup),
    )?;
    ledger.notify_everything();
    assert_eq!(counted.0.lock().map_err(|_| "poisoned")?.len(), 1);
    Ok(())
}

#[test]
fn failed_record_cannot_vouch_for_an_old_lookup() -> Result<(), Box<dyn std::error::Error>> {
    for observing in [false, true] {
        let mut setup = work();
        let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut setup))?;
        let observer: Arc<dyn super::ViewObserver> = Arc::new(Recorded::default());
        if observing {
            ledger.observe(
                Arc::downgrade(&observer),
                &mut NativeDemandCarrier::Borrowed(&mut setup),
            )?;
        }
        let stamp = ledger.stamp();
        let node = FileId::new();
        let mut denied = crate::demand::DemandWork::new(
            crate::WorkBudget {
                allocation_operations: 0,
                ..crate::WorkBudget::UNBOUNDED
            },
            &crate::CancellationToken::new(),
        );
        assert!(
            ledger
                .record(
                    &ViewChange::Node(node),
                    &mut NativeDemandCarrier::Borrowed(&mut denied)
                )
                .is_err()
        );
        assert!(!ledger.unchanged_since(&path(&["node"])?, Some(node), stamp, &mut setup)?);
    }
    Ok(())
}

/// A FUSE handler asks the ledger whether a lookup is unchanged while it holds
/// its meter. A source watcher records changes on the same meter. Before the
/// fix, a record held the map's write lock while it waited for the meter, so
/// the two threads waited on each other and the mount hung.
#[test]
fn a_reader_holding_its_meter_never_waits_on_a_recording_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| error)?;
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Native(&work))?;
    let watched = path(&["watched"])?;
    // The lookup reads the node map, which the writer records into. A record
    // into a map that already tracks a key pays for scanning it.
    let untouched = FileId::new();
    ledger.record(
        &ViewChange::Node(FileId::new()),
        &mut NativeDemandCarrier::Native(&work),
    )?;
    let stamp = ledger.stamp();
    let (finished, outcome) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        // `ViewLedger::record` charges once before it takes any map, so the
        // writer enters at the map step: the one that held the map while it
        // charged.
        let writer = {
            let (ledger, work) = (Arc::clone(&ledger), work.clone());
            move || {
                super::storage::record(
                    &ledger.nodes,
                    super::key_of(&FileId::new()),
                    ViewStamp::next(),
                    &mut NativeDemandCarrier::Native(&work),
                )
                .map_err(|error| error.to_string())
            }
        };
        let (writer, unchanged) = work.with_original_leaf(|leaf| {
            let writer = std::thread::spawn(writer);
            // Let the writer reach the point where the old code held the map
            // and waited for this meter.
            for _ in 0..100_000 {
                if writer.is_finished() || ledger.nodes.try_write().is_err() {
                    break;
                }
                std::thread::yield_now();
            }
            let unchanged = ledger
                .unchanged_since(&watched, Some(untouched), stamp, leaf)
                .map_err(|error| error.to_string());
            (writer, unchanged)
        });
        let recorded = writer.join().map_err(|_| "the writer panicked".to_owned());
        let _ = finished.send((recorded, unchanged));
    });
    let (recorded, unchanged) = outcome
        .recv_timeout(std::time::Duration::from_secs(30))
        .map_err(|_| "a ledger reader and writer deadlocked on the meter")?;
    recorded??;
    assert!(
        unchanged?,
        "an unrelated node change invalidated the lookup"
    );
    Ok(())
}
