//! One oracle over the public persisted Reader cache, without another parser.
use acyclic_harness_client::reader::{Listing, ListingProjections, RecordCache, SnapshotError};
use acyclic_stream::{CommitId, ForkCut, OwnedBytes, Provenance, StreamPath};

const BUDGET: usize = 8192;
const STREAMS: usize = 16;
const BODY: &[u8] = b"original committed payload";

fn parent() -> StreamPath {
    StreamPath::new("validation/parent").expect("valid original path")
}

fn child() -> StreamPath {
    StreamPath::new("validation/child").expect("valid original path")
}

fn populated() -> RecordCache {
    let mut cache = RecordCache::new(BUDGET, STREAMS);
    cache.observe_origin(&parent(), None);
    cache.observe_tail(&parent(), 2);
    assert!(cache.insert_record(&parent(), 1, OwnedBytes::copy_from_slice(BODY), None));
    cache.note_commit(&parent(), 1, CommitId::from_bytes([7; 32]));
    cache
}

fn inherited() -> RecordCache {
    let mut cache = populated();
    cache.observe_origin(
        &child(),
        Some(Provenance {
            source: parent(),
            inherited: ForkCut {
                forked_at: 2,
                commit_id: CommitId::from_bytes([7; 32]),
            },
        }),
    );
    cache.observe_tail(&child(), 2);
    assert!(cache.insert_listing(
        &parent(),
        0,
        Listing {
            after: None,
            next: Some(child()),
            children: vec![child()],
            fresh: true,
            projections: ListingProjections::default(),
            version: Some(CommitId::from_bytes([9; 32])),
        },
        None,
    ));
    cache
}

/// Original public writer outputs, including an inherited row and listing.
pub fn seeds() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("empty-v1", RecordCache::new(BUDGET, STREAMS).snapshot()),
        ("record-v1", populated().snapshot()),
        ("inherited-listing-v1", inherited().snapshot()),
    ]
}

fn check_with(
    input: &[u8],
    restore: impl FnOnce(&mut RecordCache, &[u8]) -> Result<(), SnapshotError>,
) -> Result<(), &'static str> {
    let mut cache = populated();
    let before = cache.snapshot();
    let charge = (
        cache.bytes(),
        cache.passed_bytes(),
        cache.backing_bytes(),
        cache.streams(),
    );
    let retained = cache
        .record(&parent(), 1)
        .ok_or("original row absent")?
        .clone();
    let pointer = retained.as_ref().as_ptr();
    match restore(&mut cache, input) {
        Err(_) => {
            if cache.snapshot() != before
                || charge
                    != (
                        cache.bytes(),
                        cache.passed_bytes(),
                        cache.backing_bytes(),
                        cache.streams(),
                    )
                || cache.record(&parent(), 1).map(|row| row.as_ref().as_ptr()) != Some(pointer)
            {
                return Err("rejected restore changed original cache or custody");
            }
        }
        Ok(()) => {
            if cache.bytes() > BUDGET || cache.streams() > STREAMS {
                return Err("restore exceeded admitted budget");
            }
            let canonical = cache.snapshot();
            let mut replay = RecordCache::new(BUDGET, STREAMS);
            replay
                .restore(&canonical)
                .map_err(|_| "writer output refused")?;
            if replay.snapshot() != canonical {
                return Err("accepted snapshot has no stable writer representation");
            }
        }
    }
    if retained.as_ref() != BODY || retained.as_ref().as_ptr() != pointer {
        return Err("restore changed original retained backing");
    }
    Ok(())
}

/// Arbitrary snapshot bytes enter the shipping restore boundary unchanged.
pub fn check(input: &[u8]) -> Result<(), &'static str> {
    check_with(input, RecordCache::restore)
}

#[cfg(test)]
#[test]
fn snapshot_contract_and_destructive_refusal_control() {
    proptest::test_runner::TestRunner::default()
        .run(
            &proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096),
            |input| {
                proptest::prop_assert_eq!(check(&input), Ok(()));
                Ok(())
            },
        )
        .expect("arbitrary persisted input");
    for (name, bytes) in seeds() {
        assert_eq!(check(&bytes), Ok(()));
        // Every proper truncation of an actual writer output is refused,
        // including late failures after staging original entries.
        for end in 0..bytes.len() {
            let mut cache = populated();
            assert!(cache.restore(&bytes[..end]).is_err());
            assert_eq!(check(&bytes[..end]), Ok(()));
        }
        let mut restored = RecordCache::new(BUDGET, STREAMS);
        assert!(restored.restore(&bytes).is_ok());
        if name != "empty-v1" {
            assert_eq!(restored.record(&parent(), 1).map(AsRef::as_ref), Some(BODY));
            assert_eq!(restored.tail(&parent()), Some(2));
            assert_eq!(
                restored.anchor(&parent(), 1),
                Some(CommitId::from_bytes([7; 32]))
            );
        }
        if name == "inherited-listing-v1" {
            assert_eq!(restored.record(&child(), 1).map(AsRef::as_ref), Some(BODY));
            let listing = restored.listing(&parent(), 0).expect("original listing");
            assert_eq!(&listing.children, &vec![child()]);
            assert!(!listing.fresh && listing.version.is_none());
        }
    }
    let destructive = |cache: &mut RecordCache, bytes: &[u8]| {
        let result = cache.restore(bytes);
        if result.is_err() {
            cache.set_budget(0);
        }
        result
    };
    assert!(check_with(b"", destructive).is_err());
}
