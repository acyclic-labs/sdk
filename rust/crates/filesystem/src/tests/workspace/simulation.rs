//! Stateful production-volume simulation. See docs/filesystem-simulation.md.
use super::*;
use acyclic_native_runtime::testing::{Schedule, fill_seeded, seeded_runtime};
use std::collections::BTreeMap;

mod lazy;

type Image = BTreeMap<String, Vec<u8>>;

fn choice() -> u8 {
    let mut byte = [0];
    assert!(fill_seeded(&mut byte), "simulation escaped shared entropy");
    byte[0]
}

async fn check<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    workspace: &Workspace<A, O>,
    expected: &Image,
) -> Result<(), Box<dyn Error>> {
    // Reopen the immutable generation, rather than trusting a checkout cache.
    let head = unbounded!(workspace, head())?;
    let reopened = unbounded!(workspace, generation(head.id()))?;
    let mut names = Vec::new();
    let mut after = None;
    loop {
        let (page, _allocation) = unbounded!(workspace, list_directory("/", after.as_ref(), 3))?;
        names.extend(
            page.entries
                .iter()
                .map(|entry| entry.name.as_bytes().to_vec()),
        );
        if !page.has_more {
            break;
        }
        after = Some(
            page.entries
                .last()
                .ok_or("empty nonterminal page")?
                .name
                .clone(),
        );
    }
    assert_eq!(
        names,
        expected
            .keys()
            .map(|path| path.as_bytes()[1..].to_vec())
            .collect::<Vec<_>>(),
        "publication exposed an unexpected or missing file"
    );
    for (path, bytes) in expected {
        assert_eq!(unbounded!(workspace, read(path, 256))?.as_ref(), bytes);
        assert_eq!(unbounded!(reopened, read(path, 256))?.as_ref(), bytes);
    }
    Ok(())
}

async fn join<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    source: &Workspace<A, O>,
    target: &Workspace<A, O>,
) -> Result<JoinOutcome<A, O>, WorkspaceError> {
    let plan = unbounded!(source.join_into(target), plan())?;
    plan.apply(
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        },
        BTreeMap::new(),
        crate::PublicationPermit::Unrestricted,
    )
    .await
}

async fn scenario(steps: usize) -> Result<(), Box<dyn Error>> {
    let simulation = crate::Simulation::new();
    let (authority, objects) = simulation.stores();
    let fs = Fs::new(authority, objects, crate::EmbeddedCapabilities::MEMORY);
    let mut parent = unbounded!(fs, create_workspace("parent"))?;
    let mut expected = Image::new();
    for step in 0..steps {
        let operation = choice() % 7;
        let path = format!("/file-{}", choice() % 4);
        let bytes = vec![choice(); 1 + usize::from(choice() % 32)];
        match operation {
            0 => {
                let mut transaction = unbounded!(parent, begin_transaction(IdempotencyKey::new()))?;
                unbounded!(transaction, write(&path, bytes.clone()))?;
                let TransactionCommit::Committed(generation) = commit(&mut transaction).await?
                else {
                    return Err("uncontended publication failed".into());
                };
                expected.insert(path, bytes);
                assert!(matches!(commit(&mut transaction).await?,
                    TransactionCommit::AlreadyCommitted(replayed) if replayed.id() == generation.id()));
            }
            1 => {
                // Both candidates capture the same head before either commits.
                let mut first = unbounded!(parent, begin_transaction(IdempotencyKey::new()))?;
                let mut second = unbounded!(parent, begin_transaction(IdempotencyKey::new()))?;
                unbounded!(first, write(&path, bytes.clone()))?;
                unbounded!(second, write(&path, vec![choice(); 33]))?;
                let reverse = choice() & 1 != 0;
                let (winner, loser) = if reverse {
                    (&mut second, &mut first)
                } else {
                    (&mut first, &mut second)
                };
                let TransactionCommit::Committed(generation) = commit(winner).await? else {
                    return Err("first conditional publication failed".into());
                };
                let winning = unbounded!(generation, read(&path, 256))?.as_ref().to_vec();
                expected.insert(path, winning);
                assert!(matches!(
                    commit(loser).await?,
                    TransactionCommit::Conflict { .. }
                ));
            }
            2 => {
                let base = unbounded!(parent, head())?;
                let child = fork_workspace(
                    &parent,
                    &format!("child-{step}"),
                    ForkOptions::from_generation(base, IdempotencyKey::new()),
                )
                .await?;
                check(&child, &expected).await?;
                let child_path = format!("/child-{step}");
                child.write(&child_path, bytes.clone().into()).await?;
                let parent_bytes = vec![choice(); 7];
                parent.write(&path, parent_bytes.clone().into()).await?;
                expected.insert(path.clone(), parent_bytes);
                assert!(matches!(
                    join(&child, &parent).await?,
                    JoinOutcome::Applied(_)
                ));
                expected.insert(child_path, bytes);
                check(&parent, &expected).await?;
                // A subsequent child delta must preserve a parent resolution of
                // a previously imported child file (incremental merge base).
                let resolved = vec![choice(); 9];
                let child_path = format!("/child-{step}");
                parent.write(&child_path, resolved.clone().into()).await?;
                expected.insert(child_path, resolved);
                let next = format!("/next-{step}");
                child.write(&next, vec![choice(); 3].into()).await?;
                let value = unbounded!(child, read(&next, 256))?.as_ref().to_vec();
                assert!(matches!(
                    join(&child, &parent).await?,
                    JoinOutcome::Applied(_)
                ));
                expected.insert(next, value);
                assert!(matches!(
                    join(&child, &parent).await?,
                    JoinOutcome::NoChanges(_)
                ));
            }
            3 => {
                // Divergent edits are an atomic conflict, never a partial join.
                let base = unbounded!(parent, head())?;
                let child = fork_workspace(
                    &parent,
                    &format!("conflict-{step}"),
                    ForkOptions::from_generation(base, IdempotencyKey::new()),
                )
                .await?;
                parent.write(&path, bytes.clone().into()).await?;
                child.write(&path, vec![choice(); 34].into()).await?;
                expected.insert(path, bytes);
                let before = unbounded!(parent, head())?.id();
                assert!(matches!(
                    join(&child, &parent).await?,
                    JoinOutcome::Conflicted { .. }
                ));
                assert_eq!(unbounded!(parent, head())?.id(), before);
            }
            4 => {
                // Crash after preparing an unpublished candidate: drop all
                // caller state, then reopen through durable storage authority.
                let mut transaction = unbounded!(parent, begin_transaction(IdempotencyKey::new()))?;
                unbounded!(transaction, write(&path, bytes))?;
                drop(transaction);
                drop(parent);
                parent = unbounded!(fs, open_workspace("parent"))?;
            }
            _ => {
                let key = IdempotencyKey::new();
                let mut transaction = unbounded!(parent, begin_transaction(key))?;
                unbounded!(transaction, write(&path, bytes.clone()))?;
                let before = unbounded!(parent, head())?.id();
                let (operation, fault) = if operation == 5 {
                    (
                        crate::SimulationOperation::ObjectFlush,
                        crate::SimulationFault::RejectBefore,
                    )
                } else {
                    (
                        crate::SimulationOperation::AuthorityAppend,
                        crate::SimulationFault::AmbiguousAuthorityAppend,
                    )
                };
                simulation.schedule_next(operation, fault)?;
                let result = commit(&mut transaction).await;
                drop(transaction);
                drop(parent);
                parent = unbounded!(fs, open_workspace("parent"))?;
                let durable = unbounded!(parent, operation_generation(key))?;
                if let Some(generation) = durable {
                    assert_eq!(unbounded!(generation, read(&path, 256))?.as_ref(), bytes);
                    expected.insert(path, bytes);
                } else {
                    assert!(
                        result.is_err(),
                        "successful commit disappeared after reopen"
                    );
                    assert_eq!(unbounded!(parent, head())?.id(), before);
                }
            }
        }
        check(&parent, &expected).await?;
    }
    lazy::scenario(steps).await?;
    Ok(())
}

fn run(seed: u64, steps: usize) -> Result<(), Box<dyn Error>> {
    let runtime = seeded_runtime(seed)?;
    runtime.block_on(async {
        let (schedule, mut events) = Schedule::seeded(seed);
        let world = schedule.scope(scenario(steps));
        tokio::pin!(world);
        loop {
            tokio::select! {
                result = &mut world => return result,
                event = events.recv() => event.ok_or("shared boundary stream closed")?.release(),
            }
        }
    })
}

#[test]
fn seeded_volume_history() -> Result<(), Box<dyn Error>> {
    let first = std::env::var("ACYCLIC_FS_SEED").map_or(Ok(0), |seed| seed.parse::<u64>())?;
    let count = std::env::var("ACYCLIC_FS_SEEDS").map_or(Ok(4), |count| count.parse::<u64>())?;
    for offset in 0..count {
        let seed = first.wrapping_add(offset);
        run(seed, 16).unwrap_or_else(|error| panic!("volume seed={seed}: {error}"));
    }
    Ok(())
}
