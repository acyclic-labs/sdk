//! Stateful production-volume simulation. See docs/filesystem-simulation.md.
use super::*;
use acyclic_native_runtime::testing::sweep::{minimize, seeds, sweep};
use acyclic_native_runtime::testing::{Schedule, fill_seeded, seeded_runtime};
use std::collections::BTreeMap;

mod lazy;

type Image = BTreeMap<String, Vec<u8>>;

fn choice() -> u8 {
    let mut byte = [0];
    assert!(fill_seeded(&mut byte), "simulation escaped shared entropy");
    byte[0]
}

#[derive(Clone, Debug)]
struct Action {
    ordinal: usize,
    kind: u8,
    slot: u8,
    bytes: Vec<u8>,
    other: u8,
    reverse: bool,
}

fn history(seed: u64, steps: usize) -> Result<Vec<Action>, Box<dyn Error>> {
    let runtime = seeded_runtime(seed)?;
    let (schedule, _events) = Schedule::seeded(seed);
    Ok(runtime.block_on(schedule.scope(async {
        (0..steps)
            .map(|ordinal| Action {
                ordinal,
                kind: choice() % 5,
                slot: choice() % 4,
                bytes: vec![choice(); 1 + usize::from(choice() % 32)],
                other: choice(),
                reverse: choice() & 1 != 0,
            })
            .collect()
    })))
}

pub(super) async fn check<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
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

async fn scenario(actions: &[Action]) -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let mut parent = unbounded!(fs, create_workspace("parent"))?;
    let mut expected = Image::new();
    for action in actions {
        let step = action.ordinal;
        let path = format!("/file-{}", action.slot);
        let bytes = action.bytes.clone();
        eprintln!("volume action={action:?}");
        match action.kind {
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
                let second_bytes = vec![action.other; 33];
                unbounded!(second, write(&path, second_bytes.clone()))?;
                let (first, second) = if action.reverse {
                    let (second, first) = tokio::join!(commit(&mut second), commit(&mut first));
                    (first?, second?)
                } else {
                    let (first, second) = tokio::join!(commit(&mut first), commit(&mut second));
                    (first?, second?)
                };
                let (generation, winning) = match (first, second) {
                    (
                        TransactionCommit::Committed(generation),
                        TransactionCommit::Conflict { .. },
                    ) => (generation, bytes),
                    (
                        TransactionCommit::Conflict { .. },
                        TransactionCommit::Committed(generation),
                    ) => (generation, second_bytes),
                    _ => return Err("racing publications did not elect exactly one winner".into()),
                };
                assert_eq!(unbounded!(generation, read(&path, 256))?.as_ref(), winning);
                expected.insert(path, winning);
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
                let parent_bytes = vec![action.other; 7];
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
                let resolved = vec![action.other; 9];
                let child_path = format!("/child-{step}");
                parent.write(&child_path, resolved.clone().into()).await?;
                expected.insert(child_path, resolved);
                let next = format!("/next-{step}");
                let value = vec![action.other; 3];
                child.write(&next, value.clone().into()).await?;
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
                child.write(&path, vec![action.other; 34].into()).await?;
                expected.insert(path, bytes);
                let before = unbounded!(parent, head())?.id();
                assert!(matches!(
                    join(&child, &parent).await?,
                    JoinOutcome::Conflicted { .. }
                ));
                assert_eq!(unbounded!(parent, head())?.id(), before);
            }
            _ => {
                // Crash after preparing an unpublished candidate: drop all
                // caller state, then reopen through durable storage authority.
                let mut transaction = unbounded!(parent, begin_transaction(IdempotencyKey::new()))?;
                unbounded!(transaction, write(&path, bytes))?;
                drop(transaction);
                drop(parent);
                parent = unbounded!(fs, open_workspace("parent"))?;
            }
        }
        check(&parent, &expected).await?;
    }
    lazy::scenario(actions).await?;
    Ok(())
}

fn run(seed: u64, actions: &[Action]) -> Result<(), Box<dyn Error>> {
    eprintln!("volume seed={seed} actions={actions:?}");
    let runtime = seeded_runtime(seed)?;
    runtime.block_on(async {
        tokio::time::pause();
        let (schedule, mut events) = Schedule::seeded(seed);
        let world = schedule.scope(scenario(actions));
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
    let swept = sweep("filesystem volume history", seeds(0..4), |seed| {
        let actions = history(seed, 16).map_err(|error| error.to_string())?;
        // Use the shared panic/error capture while shrinking as well. Keep the
        // original invariant report so a different failure cannot replace it.
        let attempt = |candidate: &[Action]| {
            sweep("volume candidate", 0..1, |_| {
                run(seed, candidate).map_err(|error| error.to_string())
            })
            .map(|_| ())
            .map_err(|failure| failure.report)
        };
        let Err(report) = attempt(&actions) else {
            return Ok(());
        };
        let minimal = minimize(actions, |candidate| {
            attempt(candidate).as_ref().err() == Some(&report)
        });
        Err(format!("{report}\nminimal actions: {minimal:#?}"))
    })?;
    eprintln!(
        "volume histories: {} seeds in {:?}",
        swept.seeds, swept.elapsed
    );
    Ok(())
}
