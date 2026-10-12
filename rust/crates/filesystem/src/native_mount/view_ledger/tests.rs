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
    // Far more distinct names than any fixed slot table could separate.
    for index in 0..20_000 {
        let created = path(&["writes", &format!("file-{index}")])?;
        ledger.record(
            &ViewChange::Bound(&created),
            &mut NativeDemandCarrier::Borrowed(&mut work),
        )?;
        ledger.record(
            &ViewChange::Node(FileId::new()),
            &mut NativeDemandCarrier::Borrowed(&mut work),
        )?;
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

/// A change reported under one spelling of a name reaches a lookup made
/// under any other spelling a case- or normalization-insensitive host
/// resolves to the same entry, whatever either name's encoding.
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
        // Composed and decomposed forms of one accented name, in
        // different cases.
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
    // Bytes that are not Unicode are keyed as bytes.
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
fn folded_keys_invalidate_only_older_lookups() -> Result<(), Box<dyn std::error::Error>> {
    let mut work = work();
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut work))?;
    let watched = path(&["watched"])?;
    let before = ledger.stamp();
    for _ in 0..=MAXIMUM_TRACKED_KEYS {
        ledger.record(
            &ViewChange::Node(FileId::new()),
            &mut NativeDemandCarrier::Borrowed(&mut work),
        )?;
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
    #[derive(Default)]
    struct Recorded(Mutex<Vec<(ViewStamp, ViewOrigin)>>);

    impl super::ViewObserver for Recorded {
        fn view_changed(&self, position: ViewStamp, origin: ViewOrigin) {
            if let Ok(mut recorded) = self.0.lock() {
                recorded.push((position, origin));
            }
        }
    }

    let mut work = work();
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut work))?;
    let recorded = Arc::new(Recorded::default());
    let observer: std::sync::Weak<Recorded> = Arc::downgrade(&recorded);
    ledger.observe(observer, &mut NativeDemandCarrier::Borrowed(&mut work))?;
    let origin = ViewOrigin::new();
    ledger.record(
        &ViewChange::Everything,
        &mut NativeDemandCarrier::Borrowed(&mut work),
    )?;
    {
        let _scope = origin.enter();
        ledger.record(
            &ViewChange::Everything,
            &mut NativeDemandCarrier::Borrowed(&mut work),
        )?;
    }
    ledger.record(
        &ViewChange::Everything,
        &mut NativeDemandCarrier::Borrowed(&mut work),
    )?;
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
struct Counted(std::sync::atomic::AtomicUsize);

impl super::ViewObserver for Counted {
    fn view_changed(&self, _: ViewStamp, _: ViewOrigin) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[test]
fn node_admission_precedes_mutation_and_completion_survives_late_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut setup = work();
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut setup))?;
    let counted: Arc<Counted> = Arc::new(Counted::default());
    let observer: Arc<dyn super::ViewObserver> = counted.clone();
    ledger.observe(
        Arc::downgrade(&observer),
        &mut NativeDemandCarrier::Borrowed(&mut setup),
    )?;
    let before = ledger.stamp();
    let node = FileId::new();
    let cancellation = crate::CancellationToken::new();
    let denied = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            peak_allocation_bytes: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    assert!(ledger.prepare_node_change(node, &denied).is_err());
    assert!(ledger.node_unchanged_since(node, before, &mut setup)?);
    assert_eq!(counted.0.load(std::sync::atomic::Ordering::Relaxed), 0);
    let original = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            items_examined: 5,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let accepted = ledger.prepare_node_change(node, &original)?;
    original.clone().charge(crate::WorkCounters {
        items_examined: 1,
        ..crate::WorkCounters::UNCHARGED
    })?;
    cancellation.cancel();
    accepted.record()?;
    assert!(!ledger.node_unchanged_since(node, before, &mut setup)?);
    assert_eq!(counted.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn publication_delivers_to_final_live_observers_and_funds_late_registration()
-> Result<(), Box<dyn std::error::Error>> {
    let mut setup = work();
    let ledger = ViewLedger::new(&mut NativeDemandCarrier::Borrowed(&mut setup))?;
    let early: Arc<dyn super::ViewObserver> = Arc::new(Counted::default());
    let dead = Arc::downgrade(&early);
    ledger.observe(dead.clone(), &mut NativeDemandCarrier::Borrowed(&mut setup))?;
    let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(work())));
    let notification = ledger.prepare_everything_notification(&original)?;
    drop(early);
    assert!(dead.upgrade().is_none());
    let counted = Arc::new(Counted::default());
    let late: Arc<dyn super::ViewObserver> = counted.clone();
    ledger.observe(
        Arc::downgrade(&late),
        &mut NativeDemandCarrier::Borrowed(&mut setup),
    )?;
    notification.record();
    assert_eq!(counted.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    let next = ledger.prepare_everything_notification(&original)?;
    drop(next);
    assert!(ledger.prepare_everything_notification(&original).is_ok());
    Ok(())
}
