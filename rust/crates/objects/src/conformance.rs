//! The one shared blob conformance suite for every provider.
//!
//! [`verify`] takes two accounts of the same disposable service. Every case
//! runs against the first; the cross-account probes also use the second and
//! fail if either account can observe, dedupe against or delete the other's
//! blobs. [`verify_medium`] checks the conditional writes every medium
//! beneath the engine must make atomic.
use crate::{
    Blobs, ByteRange, Delete, Digest, Error, Kind, Manifest, Medium, Part, Put, RootId,
    wire::ErrorCode,
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
    availability(first, second, run).await?;
    let (body, digest) = put_and_ranges(first, run).await?;
    let parent = references(first, run, digest).await?;
    batch(first, run).await?;
    let (manifest, left, right) = manifest_ranges(first, run, parent).await?;
    let (root, rooted) = delete_refusals(first, run, left).await?;
    let live = [manifest, left, right, parent, digest];
    collection(first, run, &root, rooted, live).await?;
    let seen = listing(first, live).await?;
    let usage = first
        .usage()
        .await
        .map_err(|error| fail("usage-counts-unique-bytes", error))?;
    expect!(
        "usage-counts-unique-bytes",
        u64::try_from(seen).is_ok_and(|seen| usage.blobs == seen)
            && usage.unique_bytes >= 4096 + 64 + 1000 + 1500,
        usage
    );
    isolation(first, second, run, &root, &body, [manifest, parent]).await
}

async fn availability(first: &dyn Blobs, second: &dyn Blobs, run: &str) -> Result<(), Failure> {
    // Register before inspecting an initially absent exact identity. An account
    // observing the same digest must receive none of this account's activity.
    use futures::FutureExt as _;
    let available_body = bytes(&format!("{run}/availability"), 73);
    let available_digest = Digest::of(&available_body);
    let mut available = first
        .availability(available_digest)
        .await
        .map_err(|error| fail("availability-register-before-verification", error))?;
    let mut isolated = second
        .availability(available_digest)
        .await
        .map_err(|error| fail("availability-account-isolation", error))?;
    let initial = available.cursor();
    expect!(
        "availability-register-before-verification",
        code(&first.head(available_digest).await) == Some(ErrorCode::NotFound),
        "fixture digest exists"
    );
    put(
        first,
        "availability-register-before-verification",
        available_body.clone(),
        Vec::new(),
    )
    .await?;
    let observed = available.changed().await;
    expect!(
        "availability-register-before-verification",
        matches!(observed, Ok(cursor) if cursor.incarnation() == initial.incarnation() && cursor.sequence() > initial.sequence()),
        observed
    );
    let verified = first.get(available_digest, ByteRange::FULL, 73).await;
    expect!(
        "availability-register-before-verification",
        matches!(&verified, Ok(object) if object.body() == &available_body),
        verified
    );
    expect!(
        "availability-account-isolation",
        isolated.changed().now_or_never().is_none(),
        "foreign account was notified"
    );
    // This fixture leaves the pre-existing case inventories independent.
    first
        .delete(available_digest)
        .await
        .map_err(|error| fail("availability-register-before-verification", error))?;
    drop(available);
    drop(isolated);
    Ok(())
}

/// put-created-then-already-exists-verified, head-and-exact-ranges and
/// range-and-allocation-bounds over one plain blob.
async fn put_and_ranges(first: &dyn Blobs, run: &str) -> Result<(OwnedBytes, Digest), Failure> {
    let body = bytes(&format!("{run}/plain"), 4096);
    let digest = Digest::of(&body);
    let created = first.put(body.clone(), Vec::new()).await;
    expect!(
        "put-created-then-already-exists-verified",
        matches!(&created, Ok(Put::Created { verified }) if verified.digest == digest && verified.length == 4096),
        created
    );
    let again = first.put(body.clone(), Vec::new()).await;
    expect!(
        "put-created-then-already-exists-verified",
        matches!(&again, Ok(Put::AlreadyExists { verified }) if verified.digest == digest && verified.length == 4096),
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
        matches!(&slice, Ok(object) if body.get(100..300) == Some(object.body().as_ref()) && object.start() == 100),
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
        matches!(&tail, Ok(object) if body.get(4000..) == Some(object.body().as_ref())),
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
    Ok((body, digest))
}

/// references-must-exist-and-match, returning a parent of `digest`.
async fn references(first: &dyn Blobs, run: &str, digest: Digest) -> Result<Digest, Failure> {
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
    Ok(parent)
}

/// put-batch-ordered-members-fail-alone-and-repeat-writes-nothing
async fn batch(first: &dyn Blobs, run: &str) -> Result<(), Failure> {
    let base = bytes(&format!("{run}/batch-base"), 300);
    let base_digest = Digest::of(&base);
    let child = bytes(&format!("{run}/batch-child"), 200);
    let dangling = bytes(&format!("{run}/batch-dangling"), 20);
    let [base_member, dangling_member, child_member] =
        [&base, &dangling, &child].map(|body| Digest::of(body));
    let members = [base_member, dangling_member, child_member, base_member];
    let puts = first
        .put_batch(vec![
            (base.clone(), Vec::new()),
            (dangling.clone(), vec![Digest::of(b"never stored")]),
            (child.clone(), vec![base_digest]),
            (base.clone(), Vec::new()),
        ])
        .await;
    let outcome = |put: &Result<Put, Error>, digest: Digest| match put {
        Ok(Put::Created { verified }) if verified.digest == digest => "created",
        Ok(Put::AlreadyExists { verified }) if verified.digest == digest => "exists",
        Err(error) if error.code == ErrorCode::MissingReference => "missing",
        _ => "other",
    };
    let outcomes: Vec<_> = puts
        .iter()
        .zip(members)
        .map(|(put, digest)| outcome(put, digest))
        .collect();
    expect!(
        "put-batch-ordered-members-fail-alone-and-repeat-writes-nothing",
        outcomes == ["created", "missing", "created", "exists"],
        puts
    );
    let committed = first.head(child_member).await;
    let failed = first.head(dangling_member).await;
    expect!(
        "put-batch-ordered-members-fail-alone-and-repeat-writes-nothing",
        committed.is_ok() && code(&failed) == Some(ErrorCode::NotFound),
        (committed, failed)
    );
    let before = first.usage().await;
    let repeated = first
        .put_batch(vec![(base, Vec::new()), (child, vec![base_digest])])
        .await;
    let after = first.usage().await;
    let outcomes: Vec<_> = repeated
        .iter()
        .zip([base_member, child_member])
        .map(|(put, digest)| outcome(put, digest))
        .collect();
    expect!(
        "put-batch-ordered-members-fail-alone-and-repeat-writes-nothing",
        outcomes == ["exists", "exists"]
            && matches!((&before, &after), (Ok(before), Ok(after)) if before == after),
        (repeated, before, after)
    );
    let empty = first.put_batch(Vec::new()).await;
    expect!(
        "put-batch-ordered-members-fail-alone-and-repeat-writes-nothing",
        empty.is_empty(),
        empty
    );
    Ok(())
}

/// manifest-ranges-select-intersecting-parts, returning the manifest and its
/// two parts.
async fn manifest_ranges(
    first: &dyn Blobs,
    run: &str,
    parent: Digest,
) -> Result<(Digest, Digest, Digest), Failure> {
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
        manifest
            .encode()
            .is_ok_and(|encoded| verified.digest == Digest::of(&encoded))
            && verified.length == 2500,
        verified
    );
    let spanning = first
        .get(verified.digest, ByteRange::new(900, 1100), 200)
        .await;
    let expected: Vec<u8> = left
        .iter()
        .skip(900)
        .chain(right.iter().take(100))
        .copied()
        .collect();
    expect!(
        "manifest-ranges-select-intersecting-parts",
        matches!(&spanning, Ok(object) if object.body().as_ref() == expected.as_slice() && object.head().kind == Kind::Manifest),
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
    Ok((verified.digest, left_digest, right_digest))
}

/// delete-refuses-live-references, returning the root and the blob it retains.
async fn delete_refusals(
    first: &dyn Blobs,
    run: &str,
    left_digest: Digest,
) -> Result<(RootId, Digest), Failure> {
    let referenced = first.delete(left_digest).await;
    expect!(
        "delete-refuses-live-references",
        matches!(&referenced, Ok(deletion) if deletion.outcome == Delete::Referenced && deletion.references.is_empty()),
        referenced
    );
    let root = RootId::new(format!("{run}/{}", "root".repeat(300)).into_bytes())
        .map_err(|error| fail("delete-refuses-live-references", error))?;
    let rooted_body = bytes(&format!("{run}/rooted"), 32);
    let rooted = put(
        first,
        "delete-refuses-live-references",
        rooted_body,
        vec![left_digest],
    )
    .await?;
    let retained = first.retain(&root, vec![rooted, rooted]).await;
    expect!(
        "delete-refuses-live-references",
        matches!(&retained, Ok(retained) if retained.references == 1),
        retained
    );
    let refused = first.delete(rooted).await.map(|deletion| deletion.outcome);
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
    Ok((root, rooted))
}

/// collection-keeps-retained-closures. `live` is the manifest, its two parts,
/// the parent and the plain blob.
async fn collection(
    first: &dyn Blobs,
    run: &str,
    root: &RootId,
    rooted: Digest,
    live: [Digest; 5],
) -> Result<(), Failure> {
    let [manifest, left_digest, ..] = live;
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
    let kept = first.retain(root, vec![manifest]).await;
    expect!("collection-keeps-retained-closures", kept.is_ok(), kept);
    // The pass after the one that found the orphan unreferenced keeps it.
    for _ in 0..2 {
        let pass = first.collect().await;
        expect!("collection-keeps-retained-closures", pass.is_ok(), pass);
    }
    for live in std::iter::once(rooted).chain(live) {
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
    let released = first.release(root).await.map(|released| released.existed);
    expect!(
        "collection-keeps-retained-closures",
        released == Ok(true),
        released
    );
    let released_again = first.release(root).await.map(|released| released.existed);
    expect!(
        "collection-keeps-retained-closures",
        released_again == Ok(false),
        released_again
    );
    let deleted = first.delete(rooted).await;
    expect!(
        "collection-keeps-retained-closures",
        matches!(&deleted, Ok(deletion) if deletion.outcome == Delete::Deleted && deletion.references == [left_digest]),
        deleted
    );
    let absent = first.delete(rooted).await;
    expect!(
        "collection-keeps-retained-closures",
        matches!(&absent, Ok(deletion) if deletion.outcome == Delete::Absent && deletion.references.is_empty()),
        absent
    );
    Ok(())
}

/// list-after-never-duplicates-or-skips, returning how many blobs it listed.
async fn listing(first: &dyn Blobs, live: [Digest; 5]) -> Result<usize, Failure> {
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let page = first
            .list(after, 2)
            .await
            .map_err(|error| fail("list-after-never-duplicates-or-skips", error))?;
        expect!(
            "list-after-never-duplicates-or-skips",
            page.digests.len() <= 2
                && page
                    .digests
                    .array_windows::<2>()
                    .all(|[left, right]| left < right)
                && page
                    .digests
                    .first()
                    .is_none_or(|first| after.is_none_or(|cursor| *first > cursor))
                && page.next.is_none_or(|next| {
                    after.is_none_or(|after| next > after)
                        && page.digests.last().is_none_or(|last| *last <= next)
                }),
            page
        );
        seen.extend(page.digests.iter().copied());
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    for live in live {
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
    Ok(seen.len())
}

/// cross-account-isolation: `second` cannot observe, reference, retain or
/// delete `first`'s blobs, and puts its own copy of `body` independently.
async fn isolation(
    first: &dyn Blobs,
    second: &dyn Blobs,
    run: &str,
    root: &RootId,
    body: &OwnedBytes,
    [manifest, parent]: [Digest; 2],
) -> Result<(), Failure> {
    let digest = Digest::of(body);
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
    let foreign_root = second.retain(root, vec![digest]).await;
    expect!(
        "cross-account-isolation",
        code(&foreign_root) == Some(ErrorCode::MissingReference),
        foreign_root
    );
    let foreign_delete = second.delete(digest).await.map(|deletion| deletion.outcome);
    expect!(
        "cross-account-isolation",
        foreign_delete == Ok(Delete::Absent),
        foreign_delete
    );
    let independent = second.put(body.clone(), Vec::new()).await;
    expect!(
        "cross-account-isolation",
        matches!(&independent, Ok(Put::Created { verified }) if verified.digest == digest),
        independent
    );
    let foreign_list = second.list(None, 1000).await;
    expect!(
        "cross-account-isolation",
        matches!(&foreign_list, Ok(page) if !page.digests.contains(&manifest) && !page.digests.contains(&parent)),
        foreign_list
    );
    let still_first = first.head(digest).await;
    expect!("cross-account-isolation", still_first.is_ok(), still_first);
    Ok(())
}

/// Checks that conditional mutation consumes an original source/key/version
/// fact, including the successor of a successful replacement.
pub async fn verify_medium(medium: &dyn Medium, run: &str) -> Result<(), Failure> {
    const CASE: &str = "medium-conditional-writes-use-the-original-observation";
    let key = format!("medium-conformance/{run}/key");
    let check = |step: &'static str, observed: Result<bool, crate::MediumError>, wanted: bool| {
        match observed {
            Ok(value) if value == wanted => Ok(()),
            other => Err(fail(CASE, format!("{step}: {other:?}"))),
        }
    };
    let observe = async || {
        medium
            .observe(key.clone())
            .await
            .map_err(|error| fail(CASE, format!("observe: {error}")))?
            .ok_or_else(|| fail(CASE, "missing observed key"))
    };
    if medium
        .observe(key.clone())
        .await
        .map_err(|error| fail(CASE, format!("absence: {error}")))?
        .is_some()
    {
        return Err(fail(CASE, "absence issued a conditional fact"));
    }
    let (first, second, later) = (bytes("first", 32), bytes("second", 32), bytes("later", 32));
    check("create", medium.create(&key, first.clone()).await, true)?;
    let old = observe().await?.observation;
    let old_remove = observe().await?.observation;
    let replaced = medium
        .replace(observe().await?.observation, second.clone())
        .await
        .map_err(|error| fail(CASE, format!("replace: {error}")))?
        .ok_or_else(|| fail(CASE, "matching replacement refused"))?;
    check(
        "stale replace",
        medium
            .replace(old, later.clone())
            .await
            .map(|value| value.is_some()),
        false,
    )?;
    check("stale removal", medium.remove_if(old_remove).await, false)?;
    let read = medium.read(&key, None).await;
    if !matches!(&read, Ok(Some(value)) if *value == second) {
        return Err(fail(CASE, format!("replaced value: {read:?}")));
    }
    let delayed_remove = observe().await?.observation;
    let delayed_replace = observe().await?.observation;
    // The accepted replacement itself supplies the removal's version, with no read.
    check("removal", medium.remove_if(replaced).await, true)?;
    check("recreate", medium.create(&key, later.clone()).await, true)?;
    check(
        "delayed removal",
        medium.remove_if(delayed_remove).await,
        false,
    )?;
    check(
        "delayed replace",
        medium
            .replace(delayed_replace, first.clone())
            .await
            .map(|value| value.is_some()),
        false,
    )?;
    let read = medium.read(&key, None).await;
    if !matches!(&read, Ok(Some(value)) if *value == later) {
        return Err(fail(CASE, format!("later value: {read:?}")));
    }
    let foreign = crate::MemoryMedium::default();
    check("foreign create", foreign.create(&key, later).await, true)?;
    let foreign_read = foreign
        .observe(key.clone())
        .await
        .map_err(|error| fail(CASE, format!("foreign observe: {error}")))?
        .ok_or_else(|| fail(CASE, "missing foreign key"))?;
    if !matches!(medium.remove_if(foreign_read.observation).await, Err(crate::MediumError::Failed(error)) if error.kind() == std::io::ErrorKind::InvalidInput)
    {
        return Err(fail(CASE, "foreign source accepted"));
    }
    check(
        "cleanup",
        medium.remove_if(observe().await?.observation).await,
        true,
    )?;
    Ok(())
}
