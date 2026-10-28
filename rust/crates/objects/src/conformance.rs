//! The one shared blob conformance suite for every provider.
//!
//! [`verify`] takes two accounts of the same disposable service. Every case
//! runs against the first; the cross-account probes also use the second and
//! fail if either account can observe, dedupe against or delete the other's
//! blobs.
use crate::{
    Blobs, ByteRange, Delete, Digest, Error, Kind, Manifest, Part, Put, RootId, wire::ErrorCode,
};
use acyclic_stream::OwnedBytes;

/// Case inventory, shared with every language's harness.
pub const SUITE: &[u8] = include_bytes!("../conformance/objects-v1.json");

/// The first violated case.
#[derive(Debug, thiserror::Error)]
#[error("objects conformance case {case} failed: {detail}")]
pub struct Failure {
    /// Case name from [`SUITE`].
    pub case: &'static str,
    /// What was observed.
    pub detail: String,
}

fn fail(case: &'static str, detail: impl std::fmt::Debug) -> Failure {
    Failure {
        case,
        detail: format!("{detail:?}"),
    }
}

macro_rules! expect {
    ($case:literal, $condition:expr, $observed:expr) => {
        if !$condition {
            return Err(fail($case, $observed));
        }
    };
}

fn bytes(seed: &str, length: usize) -> OwnedBytes {
    let mut out = Vec::with_capacity(length);
    let mut block = *blake3::hash(seed.as_bytes()).as_bytes();
    while out.len() < length {
        out.extend_from_slice(&block);
        block = *blake3::hash(&block).as_bytes();
    }
    out.truncate(length);
    out.into()
}

async fn put(
    blobs: &dyn Blobs,
    case: &'static str,
    body: OwnedBytes,
    references: Vec<Digest>,
) -> Result<Digest, Failure> {
    match blobs.put(body, references).await {
        Ok(Put::Created { verified } | Put::AlreadyExists { verified }) => Ok(verified.digest),
        other => Err(fail(case, other)),
    }
}

fn code<T: std::fmt::Debug>(result: &Result<T, Error>) -> Option<ErrorCode> {
    result.as_ref().err().map(|error| error.code)
}

/// Runs every case. `run` namespaces this run's content so repeated runs
/// against one service stay independent.
pub async fn verify(first: &dyn Blobs, second: &dyn Blobs, run: &str) -> Result<(), Failure> {
    // put-created-then-already-exists-verified
    let body = bytes(&format!("{run}/plain"), 4096);
    let digest = Digest::of(&body);
    let created = first.put(body.clone(), Vec::new()).await;
    expect!(
        "put-created-then-already-exists-verified",
        matches!(created, Ok(Put::Created { verified }) if verified.digest == digest && verified.length == 4096),
        created
    );
    let again = first.put(body.clone(), Vec::new()).await;
    expect!(
        "put-created-then-already-exists-verified",
        matches!(again, Ok(Put::AlreadyExists { verified }) if verified.digest == digest && verified.length == 4096),
        again
    );

    // head-and-exact-ranges
    let head = first.head(digest).await;
    expect!(
        "head-and-exact-ranges",
        matches!(&head, Ok(head) if head.kind == Kind::Blob && head.length == 4096 && head.references.is_empty()),
        head
    );
    let whole = first.get(digest, ByteRange::FULL, 4096).await;
    expect!(
        "head-and-exact-ranges",
        matches!(&whole, Ok(object) if object.body() == &body && object.start() == 0),
        whole.map(|object| object.body().len())
    );
    let slice = first.get(digest, ByteRange::new(100, 300), 200).await;
    expect!(
        "head-and-exact-ranges",
        matches!(&slice, Ok(object) if object.body()[..] == body[100..300] && object.start() == 100),
        slice.map(|object| object.body().len())
    );
    let tail = first
        .get(
            digest,
            ByteRange {
                start: 4000,
                end: None,
            },
            96,
        )
        .await;
    expect!(
        "head-and-exact-ranges",
        matches!(&tail, Ok(object) if object.body()[..] == body[4000..]),
        tail.map(|object| object.body().len())
    );

    // range-and-allocation-bounds
    let past = first.get(digest, ByteRange::new(0, 4097), u64::MAX).await;
    expect!(
        "range-and-allocation-bounds",
        code(&past) == Some(ErrorCode::RangeNotSatisfiable),
        code(&past)
    );
    let bounded = first.get(digest, ByteRange::FULL, 4095).await;
    expect!(
        "range-and-allocation-bounds",
        code(&bounded) == Some(ErrorCode::QuotaExceeded),
        code(&bounded)
    );
    let absent = first
        .get(Digest::of(run.as_bytes()), ByteRange::FULL, 1)
        .await;
    expect!(
        "range-and-allocation-bounds",
        code(&absent) == Some(ErrorCode::NotFound),
        code(&absent)
    );

    // references-must-exist-and-match
    let missing = first
        .put(
            bytes(&format!("{run}/dangling"), 10),
            vec![Digest::of(b"never stored")],
        )
        .await;
    expect!(
        "references-must-exist-and-match",
        code(&missing) == Some(ErrorCode::MissingReference),
        missing
    );
    let parent_body = bytes(&format!("{run}/parent"), 64);
    let parent = put(
        first,
        "references-must-exist-and-match",
        parent_body.clone(),
        vec![digest],
    )
    .await?;
    let conflict = first.put(parent_body, Vec::new()).await;
    expect!(
        "references-must-exist-and-match",
        code(&conflict) == Some(ErrorCode::Conflict),
        conflict
    );

    // manifest-ranges-select-intersecting-parts
    let left = bytes(&format!("{run}/left"), 1000);
    let right = bytes(&format!("{run}/right"), 1500);
    let left_digest = put(
        first,
        "manifest-ranges-select-intersecting-parts",
        left.clone(),
        Vec::new(),
    )
    .await?;
    let right_digest = put(
        first,
        "manifest-ranges-select-intersecting-parts",
        right.clone(),
        Vec::new(),
    )
    .await?;
    let manifest = Manifest {
        parts: vec![
            Part {
                digest: left_digest,
                length: 1000,
            },
            Part {
                digest: right_digest,
                length: 1500,
            },
        ],
        edges: vec![parent],
    };
    let published = first.put_manifest(manifest.clone()).await;
    let Ok(Put::Created { verified }) = published else {
        return Err(fail("manifest-ranges-select-intersecting-parts", published));
    };
    expect!(
        "manifest-ranges-select-intersecting-parts",
        verified.digest == Digest::of(&manifest.encode()) && verified.length == 2500,
        verified
    );
    let spanning = first
        .get(verified.digest, ByteRange::new(900, 1100), 200)
        .await;
    let mut expected = left[900..].to_vec();
    expected.extend_from_slice(&right[..100]);
    expect!(
        "manifest-ranges-select-intersecting-parts",
        matches!(&spanning, Ok(object) if object.body()[..] == expected[..] && object.head().kind == Kind::Manifest),
        spanning.map(|object| object.body().len())
    );
    let wrong_length = first
        .put_manifest(Manifest {
            parts: vec![Part {
                digest: left_digest,
                length: 999,
            }],
            edges: Vec::new(),
        })
        .await;
    expect!(
        "manifest-ranges-select-intersecting-parts",
        code(&wrong_length) == Some(ErrorCode::InvalidArgument),
        wrong_length
    );

    // delete-refuses-live-references
    let referenced = first.delete(left_digest).await;
    expect!(
        "delete-refuses-live-references",
        referenced == Ok(Delete::Referenced),
        referenced
    );
    let root = RootId::new(format!("{run}/root").into_bytes())
        .map_err(|error| fail("delete-refuses-live-references", error))?;
    let rooted_body = bytes(&format!("{run}/rooted"), 32);
    let rooted = put(
        first,
        "delete-refuses-live-references",
        rooted_body,
        Vec::new(),
    )
    .await?;
    let retained = first.retain(&root, vec![rooted, rooted]).await;
    expect!(
        "delete-refuses-live-references",
        matches!(retained, Ok(retained) if retained.references == 1),
        retained
    );
    let refused = first.delete(rooted).await;
    expect!(
        "delete-refuses-live-references",
        refused == Ok(Delete::Referenced),
        refused
    );
    let unretained = first.retain(&root, vec![Digest::of(b"never stored")]).await;
    expect!(
        "delete-refuses-live-references",
        code(&unretained) == Some(ErrorCode::MissingReference),
        unretained
    );

    // collection-keeps-retained-closures
    let orphan_body = bytes(&format!("{run}/orphan"), 48);
    let orphan = put(
        first,
        "collection-keeps-retained-closures",
        orphan_body,
        Vec::new(),
    )
    .await?;
    let first_pass = first.collect().await;
    expect!(
        "collection-keeps-retained-closures",
        first_pass.is_ok(),
        first_pass
    );
    let orphan_head = first.head(orphan).await;
    expect!(
        "collection-keeps-retained-closures",
        orphan_head.is_ok(),
        orphan_head
    );
    let kept = first.retain(&root, vec![verified.digest]).await;
    expect!("collection-keeps-retained-closures", kept.is_ok(), kept);
    let second_pass = first.collect().await;
    expect!(
        "collection-keeps-retained-closures",
        second_pass.is_ok(),
        second_pass
    );
    for live in [
        rooted,
        verified.digest,
        left_digest,
        right_digest,
        parent,
        digest,
    ] {
        let head = first.head(live).await;
        expect!(
            "collection-keeps-retained-closures",
            head.is_ok(),
            (live, head)
        );
    }
    let swept = first.head(orphan).await;
    expect!(
        "collection-keeps-retained-closures",
        code(&swept) == Some(ErrorCode::NotFound),
        swept
    );
    let released = first.release(&root).await;
    expect!(
        "collection-keeps-retained-closures",
        released == Ok(true),
        released
    );
    let released_again = first.release(&root).await;
    expect!(
        "collection-keeps-retained-closures",
        released_again == Ok(false),
        released_again
    );
    let deleted = first.delete(rooted).await;
    expect!(
        "collection-keeps-retained-closures",
        deleted == Ok(Delete::Deleted),
        deleted
    );
    let absent = first.delete(rooted).await;
    expect!(
        "collection-keeps-retained-closures",
        absent == Ok(Delete::Absent),
        absent
    );

    // list-after-never-duplicates-or-skips
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let page = first
            .list(after, 2)
            .await
            .map_err(|error| fail("list-after-never-duplicates-or-skips", error))?;
        expect!(
            "list-after-never-duplicates-or-skips",
            page.digests.len() <= 2 && page.digests.windows(2).all(|pair| pair[0] < pair[1]),
            page
        );
        seen.extend(page.digests.iter().copied());
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    expect!(
        "list-after-never-duplicates-or-skips",
        seen.windows(2).all(|pair| pair[0] < pair[1]),
        seen
    );
    for live in [verified.digest, left_digest, right_digest, parent, digest] {
        expect!(
            "list-after-never-duplicates-or-skips",
            seen.contains(&live),
            live
        );
    }
    let zero = first.list(None, 0).await;
    expect!(
        "list-after-never-duplicates-or-skips",
        code(&zero) == Some(ErrorCode::InvalidArgument),
        zero
    );

    // usage-counts-unique-bytes
    let usage = first
        .usage()
        .await
        .map_err(|error| fail("usage-counts-unique-bytes", error))?;
    expect!(
        "usage-counts-unique-bytes",
        usage.blobs == seen.len() as u64 && usage.unique_bytes >= 4096 + 64 + 1000 + 1500,
        usage
    );

    // cross-account-isolation
    let foreign = second.head(digest).await;
    expect!(
        "cross-account-isolation",
        code(&foreign) == Some(ErrorCode::NotFound),
        foreign
    );
    let foreign_read = second.get(digest, ByteRange::FULL, u64::MAX).await;
    expect!(
        "cross-account-isolation",
        code(&foreign_read) == Some(ErrorCode::NotFound),
        code(&foreign_read)
    );
    let foreign_reference = second
        .put(bytes(&format!("{run}/foreign"), 8), vec![digest])
        .await;
    expect!(
        "cross-account-isolation",
        code(&foreign_reference) == Some(ErrorCode::MissingReference),
        foreign_reference
    );
    let foreign_root = second.retain(&root, vec![digest]).await;
    expect!(
        "cross-account-isolation",
        code(&foreign_root) == Some(ErrorCode::MissingReference),
        foreign_root
    );
    let foreign_delete = second.delete(digest).await;
    expect!(
        "cross-account-isolation",
        foreign_delete == Ok(Delete::Absent),
        foreign_delete
    );
    let independent = second.put(body.clone(), Vec::new()).await;
    expect!(
        "cross-account-isolation",
        matches!(independent, Ok(Put::Created { verified }) if verified.digest == digest),
        independent
    );
    let foreign_list = second.list(None, 1000).await;
    expect!(
        "cross-account-isolation",
        matches!(&foreign_list, Ok(page) if !page.digests.contains(&verified.digest) && !page.digests.contains(&parent)),
        foreign_list
    );
    let still_first = first.head(digest).await;
    expect!("cross-account-isolation", still_first.is_ok(), still_first);
    Ok(())
}
