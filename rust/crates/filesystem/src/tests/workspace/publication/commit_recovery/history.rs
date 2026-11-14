//! Stateful crash histories use the original physical-cut subprocess driver.
use super::*;
use acyclic_native_runtime::testing::{fill_seeded, seeded_runtime};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    acknowledged: BTreeMap<String, Vec<u8>>,
    candidate: BTreeMap<String, Vec<u8>>,
    key: IdempotencyKey,
    acknowledged_key: bool,
}

impl Checkpoint {
    fn save(&self, root: &Path) -> TestResult {
        // Observer data, outside the intercepted SDK storage path. A process
        // cut occurs only at shared-core events, never inside this observation.
        std::fs::write(root.join("history-oracle"), serde_json::to_vec(self)?)?;
        Ok(())
    }
}

fn random() -> u8 {
    let mut byte = [0];
    assert!(fill_seeded(&mut byte));
    byte[0]
}

pub(super) async fn publish(
    root: &Path,
    workspace: &crate::Workspace<crate::LocalAuthorityBackend, crate::LocalObjectBackend>,
) -> TestResult {
    let mut acknowledged = BTreeMap::new();
    for _ in 0..8 {
        let key = IdempotencyKey::new();
        let slot = random() % 4;
        let bytes = vec![random(); 1 + usize::from(random() % 32)];
        let left = format!("/slot-{slot}-left");
        let right = format!("/slot-{slot}-right");
        let mut candidate = acknowledged.clone();
        candidate.insert(left.clone(), bytes.clone());
        candidate.insert(right.clone(), bytes.clone());
        Checkpoint {
            acknowledged: acknowledged.clone(),
            candidate: candidate.clone(),
            key,
            acknowledged_key: false,
        }
        .save(root)?;
        let mut transaction = unbounded!(workspace, begin_transaction(key))?;
        unbounded!(transaction, write(&left, bytes.clone()))?;
        unbounded!(transaction, write(&right, bytes))?;
        assert!(matches!(
            commit(&mut transaction).await?,
            TransactionCommit::Committed(_)
        ));
        acknowledged = candidate;
        Checkpoint {
            acknowledged: acknowledged.clone(),
            candidate: acknowledged.clone(),
            key,
            acknowledged_key: true,
        }
        .save(root)?;
        crate::workspace::tests::simulation::check(workspace, &acknowledged).await?;
    }
    Ok(())
}

async fn seed(root: &Path) -> TestResult {
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    drop(unbounded!(fs, create_workspace("repo"))?);
    close_local(fs).await
}

async fn recover(root: &Path) -> TestResult {
    let checkpoint: Checkpoint =
        serde_json::from_slice(&std::fs::read(root.join("history-oracle"))?)?;
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let workspace = unbounded!(fs, open_workspace("repo"))?;
    let published = unbounded!(workspace, operation_generation(checkpoint.key))?.is_some();
    assert!(
        !checkpoint.acknowledged_key || published,
        "lost an acknowledged operation identity"
    );
    let expected = if published {
        &checkpoint.candidate
    } else {
        &checkpoint.acknowledged
    };
    crate::workspace::tests::simulation::check(&workspace, expected).await?;
    let head = unbounded!(workspace, head())?;
    let child = fork_workspace(
        &workspace,
        "recovered-child",
        ForkOptions::from_generation(head, IdempotencyKey::new()),
    )
    .await?;
    child.write_text("/successor", "live").await?;
    let plan = unbounded!(child.join_into(&workspace), plan())?;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            BTreeMap::new(),
            crate::PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Applied(_)
    ));
    let mut expected = expected.clone();
    expected.insert("/successor".into(), b"live".to_vec());
    crate::workspace::tests::simulation::check(&workspace, &expected).await?;
    drop(plan);
    drop(child);
    drop(workspace);
    close_local(fs).await
}

#[test]
fn seeded_physical_crash_history() -> TestResult {
    for seed_value in 0..2 {
        let runtime = seeded_runtime(seed_value)?;
        runtime
            .block_on(async {
                let directory = tempfile::tempdir()?;
                let baseline = directory.path().join("baseline");
                seed(&baseline).await?;
                let count =
                    run_seeded_child(&baseline, PublicationKind::History, None, seed_value)?;
                assert!(count > 0, "history bypassed physical fault sites");
                recover(&baseline).await?;
                let (schedule, _events) = Schedule::seeded(seed_value);
                let (ordinal, fault) = schedule
                    .scope(async {
                        (
                            usize::from(random()) % count,
                            ["before", "after", "torn"][usize::from(random() % 3)],
                        )
                    })
                    .await;
                let cut = directory.path().join("cut");
                seed(&cut).await?;
                run_seeded_child(
                    &cut,
                    PublicationKind::History,
                    Some((ordinal, fault)),
                    seed_value,
                )?;
                recover(&cut).await?;
                eprintln!("physical history seed={seed_value} cut={ordinal}/{count} fault={fault}");
                Ok::<_, Box<dyn Error>>(())
            })
            .unwrap_or_else(|error| panic!("physical history seed={seed_value}: {error}"));
    }
    Ok(())
}
