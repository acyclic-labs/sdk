use super::*;
use crate::{
    goal::GoalStatus,
    task_volume::{Cursor, FileValue, Generation, SCHEMA, Snapshot},
};
use acyclic_stream::BoxProviderFuture;
use futures::FutureExt as _;
use proptest::prelude::*;

struct Source {
    goal: Option<String>,
    count: u32,
    fault: Option<u8>,
    pending: bool,
    closed: bool,
    revision: Option<crate::OperationId>,
    publication: crate::OperationId,
}

impl Source {
    fn goal(text: &str) -> Self {
        Self {
            goal: Some(text.into()),
            count: 0,
            fault: None,
            pending: false,
            closed: false,
            revision: None,
            publication: crate::OperationId::from_bytes([1; 16]),
        }
    }
    fn head(count: u32) -> Self {
        Self {
            goal: None,
            count,
            fault: None,
            pending: false,
            closed: false,
            revision: None,
            publication: crate::OperationId::from_bytes([1; 16]),
        }
    }
    fn acknowledging(mut self, publication: u8) -> Self {
        self.revision = Some(crate::OperationId::from_bytes([publication; 16]));
        self
    }
    #[cfg(feature = "tui")]
    fn published(mut self, publication: u8) -> Self {
        self.publication = crate::OperationId::from_bytes([publication; 16]);
        self
    }
    fn snapshot(&self, query: Query) -> Snapshot {
        let volume = [if self.goal.is_some() { 1 } else { 2 }; 32];
        let cursor = Cursor {
            volume,
            epoch: uuid::Uuid::nil(),
            schema: SCHEMA,
            query: *blake3::hash(&serde_json::to_vec(&query).unwrap()).as_bytes(),
            position: 1,
            accepted: 1,
            durable: 1,
            commit: [3; 32],
        };
        let file = if let Some(goal) = &self.goal {
            let mut file = FileValue::new(FileKey::Goal, goal.as_bytes().to_vec());
            file.publication = Some(self.publication);
            file
        } else {
            let mut head: serde_json::Value = serde_json::from_slice(include_bytes!(
                "../../../../harness-fuzz/corpus/records/turn"
            ))
            .unwrap();
            head["automatic_turns"] = self.count.into();
            head["goal_revision"] = serde_json::to_value(self.revision).unwrap();
            FileValue::new(FileKey::RequestHead, serde_json::to_vec(&head).unwrap())
        };
        let mut snapshot = Snapshot {
            generation: Generation {
                volume,
                epoch: cursor.epoch,
                position: cursor.position,
                commit: cursor.commit,
            },
            cursor,
            files: vec![file],
        };
        match self.fault {
            Some(1) => snapshot.cursor.schema += 1,
            Some(2) => snapshot.cursor.query[0] ^= 1,
            Some(3) => snapshot.generation.volume[0] ^= 1,
            Some(4) => snapshot.generation.position += 1,
            Some(5) => snapshot.files[0].key = FileKey::Goal,
            Some(6) => snapshot.files.push(snapshot.files[0].clone()),
            _ => {}
        }
        snapshot
    }
}

impl FileQuerySource for Source {
    fn observe_files(
        &self,
        query: Query,
        _: Option<Cursor>,
        _: usize,
        _: usize,
    ) -> BoxProviderFuture<'_, crate::task_volume::Result<Observation>> {
        Box::pin(async move {
            if self.pending {
                return futures::future::pending().await;
            }
            if self.fault == Some(7) {
                return Err(crate::task_volume::Error::Unauthorized);
            }
            Ok(Observation {
                snapshot: (self.fault != Some(0)).then(|| self.snapshot(query)),
                reset: false,
                changes: if self.closed {
                    Box::pin(futures::stream::empty())
                } else {
                    Box::pin(futures::stream::pending())
                },
            })
        })
    }
}

// Negative control: publishing the first cut before validating the second used
// to be sufficient for single-file selection. That sequence tears a joined view.
async fn partial_select(views: &mut ClientViews, goal: &Source, head: &Source) -> Result<()> {
    for (query, source) in [
        (Query::File(FileKey::Goal), goal),
        (Query::File(FileKey::RequestHead), head),
    ] {
        let observed = source.observe_files(query.clone(), None, 1, 4096).await?;
        views.query_snapshot(
            query,
            observed
                .snapshot
                .ok_or_else(|| Error::Conflict("missing".into()))?,
            1,
            4096,
        )?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Every u32 counter and accepted status survives selection. Every invalid
    // second cut leaves the complete previous selection untouched.
    #[test]
    fn goal_selection_preserves_states_and_rolls_back_rejected_cuts(
        count in any::<u32>(), status in 0usize..3, fault in 0u8..8,
    ) {
        futures::executor::block_on(async {
            let mut views = new_views();
            let statuses = [("active", GoalStatus::Active), ("done", GoalStatus::Done), ("blocked", GoalStatus::Blocked)];
            let text = format!("---\nstatus: {}\nreason: waiting\n---\nold", statuses[status].0);
            let head = Source::head(count).acknowledging(1);
            let goal = Source::goal(&text);
            let _follow = views.select_goal_view(&goal, &head, 4096).await.unwrap();
            let identity = views.selections().find(|(_, view)| view.query() == &Query::File(FileKey::Goal)).unwrap().0;
            let selected = views.goal_view(identity.0, identity.1).unwrap();
            assert_eq!(selected.progress.unwrap().continuations, count);
            assert!(matches!(selected.goal, super::super::GoalFileView::Present { goal }
                if goal.status == statuses[status].1 && goal.reason.as_deref() == Some("waiting")));
            let before = views.clone();
            let changed = Source::goal("new objective");
            let mut invalid = Source::head(count);
            invalid.fault = Some(fault);
            assert!(views.select_goal_view(&changed, &invalid, 4096).await.is_err());
            assert_eq!(views, before);
            let mut broken = before.clone();
            assert!(partial_select(&mut broken, &changed, &invalid).await.is_err());
            assert_ne!(broken, before, "negative control did not detect a partial selection");
        });
    }
}

#[cfg(feature = "tui")]
mod interleavings;

#[test]
fn cancellation_before_both_cuts_are_ready_keeps_the_previous_view() {
    let mut views = new_views();
    let before = views.clone();
    let mut head = Source::head(2);
    head.pending = true;
    assert!(
        views
            .select_goal_view(&Source::goal("new"), &head, 4096)
            .now_or_never()
            .is_none()
    );
    assert_eq!(views, before);
}

#[test]
fn either_required_follow_ending_demands_a_reset() {
    for head_ends in [false, true] {
        futures::executor::block_on(async {
            let mut views = new_views();
            let mut goal = Source::goal("new");
            let mut head = Source::head(2);
            goal.closed = !head_ends;
            head.closed = head_ends;
            let mut changes = views.select_goal_view(&goal, &head, 4096).await.unwrap();
            assert!(matches!(
                changes.next().now_or_never(),
                Some(Some(Err(crate::task_volume::Error::ResetRequired)))
            ));
        });
    }
}

fn new_views() -> ClientViews {
    ClientViews::new(
        acyclic_stream::StreamPath::new("swarm").unwrap(),
        crate::client::ClientViewLimits {
            agents: 0,
            selected: 0,
            overview_values: 0,
            overview_bytes: 0,
            query: crate::client::QueryViewLimits {
                files: 0,
                segments: 0,
                bytes: 0,
            },
        },
    )
    .unwrap()
}
