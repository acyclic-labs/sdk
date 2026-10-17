//! Reused publication proofs against the full-walk oracle.

use super::*;
use crate::kernel::closure::{CachedProofs, ProofContext, ProofMeter, prove_generation_closure};
use crate::storage::{AppendOutcome, ObjectId, ObjectKind};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

const FILES: [&str; 6] = ["/a", "/b", "/d0/x", "/d0/y", "/d1/z", "/d1/d2/w"];

#[derive(Clone, Debug)]
enum Change {
    Write { file: usize, bytes: usize, seed: u8 },
    Remove { file: usize },
}

fn change() -> impl Strategy<Value = Change> {
    prop_oneof![
        3 => (0..FILES.len(), prop_oneof![0..64_usize, 4_000..9_000_usize], any::<u8>())
            .prop_map(|(file, bytes, seed)| Change::Write { file, bytes, seed }),
        1 => (0..FILES.len()).prop_map(|file| Change::Remove { file }),
    ]
}

fn committed() -> AppendOutcome {
    AppendOutcome::Committed(crate::foundation::DurableCommit {
        epoch: crate::foundation::Epoch::GENESIS,
        sequence: crate::foundation::Sequence::new(1),
        operation_id: crate::foundation::OperationId::from_bytes([1; 16]),
        fingerprint: crate::Digest::ZERO,
        previous_digest: crate::Digest::ZERO,
        digest: crate::Digest::ZERO,
        payload: bytes::Bytes::new(),
        settled_at_micros: None,
    })
}

/// Proves each committed generation in order and checks it against a full walk.
///
/// A generation whose `published` flag is false stands for a failed append:
/// its proof is dropped, so the next proof must be a full walk.
async fn check(transactions: Vec<Vec<Change>>, published: Vec<bool>) -> Result<u64, Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("proof-cache"))?;
    // A settled tree, so an append changes a small part of its closure.
    let mut bulk = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    for directory in 0..4 {
        unbounded!(bulk, create_dir_all(&format!("/bulk/{directory}")))?;
        for file in 0..8 {
            let path = format!("/bulk/{directory}/{file}");
            let body = bytes::Bytes::from(vec![directory * 8 + file; 5_000]);
            unbounded!(bulk, write(&path, body))?;
        }
    }
    let TransactionCommit::Committed(settled) = commit(&mut bulk).await? else {
        return Err("the settled tree did not commit".into());
    };
    let mut generations = vec![settled.id()];
    let mut present = BTreeMap::new();
    for changes in &transactions {
        let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        let before = present.clone();
        for change in changes {
            match *change {
                Change::Write { file, bytes, seed } => {
                    let path = FILES[file];
                    if let Some((parent, _)) = path.rsplit_once('/')
                        && !parent.is_empty()
                    {
                        unbounded!(transaction, create_dir_all(parent))?;
                    }
                    let body = (0..bytes)
                        .map(|index| seed.wrapping_add(index as u8))
                        .collect::<Vec<_>>();
                    unbounded!(transaction, write(path, bytes::Bytes::from(body.clone())))?;
                    present.insert(file, body);
                }
                Change::Remove { file } => {
                    if present.remove(&file).is_some() {
                        unbounded!(transaction, remove(FILES[file]))?;
                    }
                }
            }
        }
        if present == before {
            continue;
        }
        let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
            return Err("a sole writer's transaction did not commit".into());
        };
        generations.push(generation.id());
    }
    let store = fs.objects();
    let limits = crate::facade::closure_limits(workspace.volume.config);
    let cache = Mutex::new(CachedProofs::default());
    let (mut claimed, mut live_full, mut live_listed) =
        (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    let (mut increments, mut previous_published) = (0, false);
    for (index, generation) in generations.into_iter().enumerate() {
        let root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation.digest(),
        };
        let cancellation = CancellationToken::new();
        let meter = ProofMeter::standalone(WorkBudget::UNBOUNDED, &cancellation)?;
        let (proof, guard, pending) = ProofContext::new(store, limits, meter)
            .prove_cached(root, Some(&cache))
            .await?;
        let full = prove_generation_closure(store, root, limits, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?;
        assert_eq!(proof.root, full.root);
        assert_eq!(proof.generation_id, full.generation_id);
        assert_eq!(proof.file_count, full.file_count);
        assert_eq!(proof.logical_file_bytes, full.logical_file_bytes);
        let full_set = full.objects.iter().copied().collect::<BTreeSet<_>>();
        let listed = proof.objects.iter().copied().collect::<BTreeSet<_>>();
        assert!(listed.is_subset(&full_set), "a proof listed an object outside its closure");
        if listed == full_set {
            claimed = full_set.clone();
        } else {
            assert!(previous_published, "only a published parent's proof is reused");
            increments += 1;
            claimed.extend(listed.iter().copied());
            assert!(full_set.is_subset(&claimed), "a reused proof omitted a live object");
        }
        // What collection protects (every published listing) is exactly the live set.
        live_full.extend(full_set);
        live_listed.extend(listed);
        assert_eq!(live_full, live_listed);
        drop(guard);
        previous_published = published.get(index).copied().unwrap_or(true);
        let pending = pending.ok_or("a cached publication proof must yield a ticket")?;
        if previous_published {
            pending.publish(&committed());
        }
    }
    Ok(increments)
}

fn run(transactions: Vec<Vec<Change>>, published: Vec<bool>) -> Result<u64, Box<dyn Error>> {
    tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(check(transactions, published))
}

#[test]
fn appended_files_reuse_the_published_parent_proof() -> Result<(), Box<dyn Error>> {
    let writes = (0..4)
        .map(|file| {
            vec![Change::Write {
                file,
                bytes: 16,
                seed: file as u8,
            }]
        })
        .collect();
    assert_eq!(run(writes, Vec::new())?, 4);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn reused_proofs_equal_the_full_walk(
        transactions in prop::collection::vec(prop::collection::vec(change(), 1..4), 1..8),
        published in prop::collection::vec(prop::bool::weighted(0.8), 8),
    ) {
        run(transactions, published).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

/// Heap and logical work proving the commit that adds `/next`, after `turns`
/// commits grew `/log` by one chunk each.
///
/// Every page that commit touches has the same encoded size for any `turns`;
/// only the untouched closure of `/log` grows. Without the parent's published
/// proof, the prover falls back to the full walk every commit paid before.
fn next_proof(turns: u8, parent_published: bool) -> Result<(u64, u64, crate::WorkCounters), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let fs = Fs::memory();
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.limits.maximum_object_bytes = 4_096;
    let workspace =
        runtime.block_on(async { unbounded!(fs, create_workspace_with_config("turns", config)) })?;
    let turn = |key: u8, path: &'static str, body: Vec<u8>| {
        let workspace = &workspace;
        async move {
            let key = IdempotencyKey::from_bytes([key; 16]);
            let mut transaction = unbounded!(workspace, begin_transaction(key))?;
            unbounded!(transaction, write(path, bytes::Bytes::from(body)))?;
            match commit(&mut transaction).await? {
                TransactionCommit::Committed(generation) => Ok::<_, Box<dyn Error>>(ObjectId {
                    kind: ObjectKind::GenerationRoot,
                    digest: generation.id().digest(),
                }),
                _ => Err("a sole writer's turn did not commit".into()),
            }
        }
    };
    let mut parent = None;
    for index in 0..turns {
        let body = (0..=index)
            .flat_map(|chunk| (0..4_096_u16).map(move |byte| (byte as u8) ^ chunk))
            .collect();
        parent = Some(runtime.block_on(turn(index, "/log", body))?);
    }
    let parent = parent.ok_or("no turns")?;
    let next = runtime.block_on(turn(0xEE, "/next", vec![7; 64]))?;
    let (store, limits) = (fs.objects(), crate::facade::closure_limits(workspace.volume.config));
    let cache = Mutex::new(CachedProofs::default());
    let cancellation = CancellationToken::new();
    let prove = |root| {
        let meter = ProofMeter::standalone(WorkBudget::UNBOUNDED, &cancellation)?;
        runtime
            .block_on(ProofContext::new(store, limits, meter).prove_cached(root, Some(&cache)))
            .map_err(Box::<dyn Error>::from)
    };
    let (_, _, pending) = prove(parent)?;
    if parent_published {
        pending.ok_or("a cached proof must yield a ticket")?.publish(&committed());
    }
    let mut proved = None;
    let heap = acyclic_test_support::measure(|| proved = Some(prove(next)));
    let (proof, _, _) = proved.ok_or("the proof did not run")??;
    Ok((heap.count_total, heap.bytes_total, proof.work))
}

#[test]
fn next_proof_work_is_independent_of_prior_turns() -> Result<(), Box<dyn Error>> {
    let (few, many) = (next_proof(8, true)?, next_proof(24, true)?);
    // Negative control: the full walk grows with the closure the proof skipped.
    let (few_full, many_full) = (next_proof(8, false)?, next_proof(24, false)?);
    assert_eq!(few, many, "the next proof's allocations, bytes or work grew with history");
    assert!(
        many_full.0 > few_full.0 && many_full.1 > few_full.1,
        "the full walk must grow with history: {few_full:?} vs {many_full:?}"
    );
    Ok(())
}
