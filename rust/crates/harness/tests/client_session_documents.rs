//! Real task-volume snapshot/follow projections for the client document panels.

use acyclic_harness::{
    client::DocumentViews,
    task_volume::{FileKey, PlanName, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::{FutureExt, StreamExt};
use std::sync::Arc;

fn write(kind: u8, text: &str) -> Write {
    let key = if kind == 0 {
        FileKey::Goal
    } else {
        FileKey::Plan(PlanName::new(format!("plan-{kind}")).unwrap())
    };
    Write::new(key, text.as_bytes().to_vec())
}

async fn publication(
    mut files: Vec<Write>,
    removed: Vec<FileKey>,
) -> acyclic_harness::task_volume::Change {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("document-publication").unwrap(),
    )
    .await
    .unwrap();
    acyclic_harness::task_volume::Change {
        file: files.pop(),
        files,
        removed,
        cursor: volume.snapshot(&Query::Root, 8).await.unwrap().cursor,
    }
}

#[tokio::test]
async fn bulk_documents_remain_bound_to_the_tracked_authority() {
    use acyclic_harness::{client::ViewUpdate, wire};
    let mut state = super::session();
    let change = publication(vec![write(0, "goal"), write(1, "plan")], vec![]).await;
    let before = state.views().clone();
    assert!(
        state
            .apply_update(ViewUpdate::DocumentChange {
                authority: wire::Authority {
                    id: "untracked".into(),
                    ..super::owner()
                },
                change,
            })
            .is_err()
    );
    assert_eq!(state.views(), &before);
    let change = publication(vec![write(0, "goal"), write(1, "plan")], vec![]).await;
    state
        .apply_update(ViewUpdate::DocumentChange {
            authority: super::owner(),
            change,
        })
        .unwrap();
    let documents = &state.views().documents["AGGREGATE_KIND_TASK:task"];
    assert_eq!(documents.goal.as_deref(), Some("goal"));
    assert_eq!(documents.plans.len(), 1);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: a bulk replacement/removal equals its final file snapshot;
    // any malformed replacement preserves every previous document atomically.
    #[test]
    fn document_publications_are_atomic(
        operations in proptest::collection::vec((0u8..4, proptest::bool::ANY, ".{0,20}"), 0..12),
        invalid in proptest::bool::ANY,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let operations: std::collections::BTreeMap<_, _> = operations.into_iter().map(|(kind, remove, text)| (kind, (remove, text))).collect();
            let mut live = DocumentViews::default();
            for kind in 0..4 { live.apply(&write(kind, "original")).unwrap(); }
            let before = live.clone();
            let mut expected = before.clone();
            let (mut files, mut removed) = (Vec::new(), Vec::new());
            for (kind, (remove, text)) in operations {
                let file = write(kind, &text);
                if remove {
                    match &file.key {
                        FileKey::Goal => expected.goal = None,
                        FileKey::Plan(name) => { expected.plans.remove(name); }
                        _ => unreachable!(),
                    }
                    removed.push(file.key);
                } else {
                    expected.apply(&file).unwrap();
                    files.push(file);
                }
            }
            let mut change = publication(files, removed).await;
            if invalid { change.files.push(Write::new(FileKey::Plan(PlanName::new("bad").unwrap()), vec![0xff])); }
            let result = live.apply_change(&change);
            if invalid {
                proptest::prop_assert!(result.is_err());
                proptest::prop_assert_eq!(live, before);
            } else {
                proptest::prop_assert!(result.is_ok());
                proptest::prop_assert_eq!(live, expected);
            }
            Ok(())
        })?;
    }
}

async fn snapshot_plus_follow(events: &[(u8, String)], cut: usize) {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("documents").unwrap(),
    )
    .await
    .unwrap();
    let mut generation = volume.snapshot(&Query::Root, 4).await.unwrap().generation;
    let cut = cut.min(events.len());
    for (kind, text) in &events[..cut] {
        generation = volume
            .append(generation, &write(*kind, text))
            .await
            .unwrap();
    }
    let snapshot = volume.snapshot(&Query::Root, 4).await.unwrap();
    let mut live = DocumentViews::default();
    live.replace(&Query::Root, &snapshot).unwrap();
    let mut follow = volume.follow(Query::Root, snapshot.cursor).await.unwrap();
    for (kind, text) in &events[cut..] {
        for _ in 0..2 {
            let before = generation;
            generation = volume
                .append(generation, &write(*kind, text))
                .await
                .unwrap();
            let next = tokio::task::unconstrained(follow.next()).now_or_never();
            if generation == before {
                assert!(next.is_none(), "identical files must not publish a change");
            } else {
                let change = next
                    .expect("accepted cut must be immediately followed")
                    .unwrap()
                    .unwrap();
                live.apply(&change.file.unwrap()).unwrap();
            }
        }
    }
    let final_snapshot = volume.snapshot(&Query::Root, 4).await.unwrap();
    let mut replay = DocumentViews::default();
    replay.replace(&Query::Root, &final_snapshot).unwrap();
    assert_eq!(live, replay);
}

#[tokio::test]
async fn goal_and_plans_follow_ordinary_writes_without_polling() {
    snapshot_plus_follow(
        &[
            (0, "---\nstatus: active\n---\nFinish rewrite".into()),
            (1, "# Layout\n- [ ] compose".into()),
            (2, "# Test\n- [ ] PTY".into()),
            (1, "# Layout\n- [x] compose".into()),
            (0, "---\nstatus: done\n---\nFinished".into()),
        ],
        2,
    )
    .await;
}

#[tokio::test]
async fn reset_removes_stale_selected_files_and_preserves_other_panels() {
    let mut views = DocumentViews::default();
    views.apply(&write(0, "goal")).unwrap();
    views.apply(&write(1, "old plan")).unwrap();
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("empty").unwrap(),
    )
    .await
    .unwrap();
    let empty = volume.snapshot(&Query::Plans, 4).await.unwrap();
    views.replace(&Query::Plans, &empty).unwrap();
    assert_eq!(views.goal.as_deref(), Some("goal"));
    assert!(views.plans.is_empty());
    let mut broken = volume.snapshot(&Query::Root, 4).await.unwrap();
    broken.files = vec![
        write(0, "replacement"),
        Write::new(FileKey::Plan(PlanName::new("bad").unwrap()), vec![0xff]),
    ];
    let before = views.clone();
    assert!(views.replace(&Query::Root, &broken).is_err());
    assert_eq!(views, before);
    assert!(views.apply(&broken.files[1]).is_err());
    assert_eq!(views, before);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Property: for every cut, folding the real ordered follow yields exactly
    // the same GOAL.md and plans as a new snapshot at the final publication cut.
    #[test]
    fn every_snapshot_follow_cut_matches_the_current_files(
        events in proptest::collection::vec((0u8..4, ".{0,12}"), 0..12),
        cut in 0usize..16,
    ) {
        futures::executor::block_on(snapshot_plus_follow(&events, cut));
    }
}

#[tokio::test]
async fn platform_file_updates_reach_the_shared_client_views() {
    use super::{Host, session};
    use acyclic_harness::client::{HarnessClient, ViewUpdate};
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("client-documents").unwrap(),
    )
    .await
    .unwrap();
    let snapshot = volume.snapshot(&Query::Plans, 4).await.unwrap();
    let mut source = futures::stream::iter([
        Ok(ViewUpdate::DocumentSnapshot {
            authority: super::owner(),
            query: Query::Plans,
            snapshot,
        }),
        Ok(ViewUpdate::DocumentFile {
            authority: super::owner(),
            file: write(0, "current goal"),
        }),
        Ok(ViewUpdate::DocumentFile {
            authority: super::owner(),
            file: write(1, "current plan"),
        }),
    ]);
    let mut client = HarnessClient::connect(Arc::new(Host::default()), session())
        .await
        .unwrap();
    while client.next_update(&mut source).await.unwrap() {}
    assert_eq!(
        client.session().views().documents["AGGREGATE_KIND_TASK:task"]
            .goal
            .as_deref(),
        Some("current goal")
    );
    assert_eq!(
        client.session().views().documents["AGGREGATE_KIND_TASK:task"].plans
            [&PlanName::new("plan-1").unwrap()],
        "current plan"
    );
}

#[test]
fn private_documents_stay_with_their_tracked_agent_and_fail_atomically() {
    use acyclic_harness::{client::ViewUpdate, wire};
    let mut state = super::session();
    let other = wire::Authority {
        id: "child".into(),
        ..super::owner()
    };
    state
        .track(wire::ReplayCursor {
            authority: Some(other.clone()),
            generation: "child-generation".into(),
            revision: 0,
        })
        .unwrap();
    for (authority, text) in [(super::owner(), "root goal"), (other.clone(), "child goal")] {
        state
            .apply_update(ViewUpdate::DocumentFile {
                authority,
                file: write(0, text),
            })
            .unwrap();
    }
    assert_eq!(
        state.views().documents["AGGREGATE_KIND_TASK:task"]
            .goal
            .as_deref(),
        Some("root goal")
    );
    assert_eq!(
        state.views().documents["AGGREGATE_KIND_TASK:child"]
            .goal
            .as_deref(),
        Some("child goal")
    );
    let before = state.views().clone();
    for (authority, file) in [
        (
            wire::Authority {
                id: "untracked".into(),
                ..other.clone()
            },
            write(0, "wrong goal"),
        ),
        (other, Write::new(FileKey::Goal, vec![0xff])),
    ] {
        assert!(
            state
                .apply_update(ViewUpdate::DocumentFile { authority, file })
                .is_err()
        );
        assert_eq!(state.views(), &before);
    }
}
