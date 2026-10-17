use super::*;

impl<S: ClientStateStore> ClientSession<S> {
    pub(in crate::client) fn replace_task_views(
        &mut self,
        replacement: Self,
        owner: &wire::Authority,
    ) {
        let owner = authority_key(owner);
        self.views.transcripts.retain(|key, _| key == &owner);
        // Task-scoped selections belong to the previous task; global folds
        // (task overviews, sessions, inputs) keep following across switches.
        self.views
            .queries
            .retain(|_, view| view.query().scope() == crate::task_volume::QueryScope::Global);
        self.views.queries.extend(replacement.views.queries);
        self.views.transcripts.extend(replacement.views.transcripts);
        self.views.task_folds = replacement.views.task_folds;
        self.views.tools = replacement.views.tools;
    }

    pub(in crate::client) fn project_commands(
        &mut self,
        projection: &mut crate::client::command_output::Projection,
        active: Option<crate::TaskId>,
    ) {
        projection.apply(&mut self.views, active);
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use crate::task_volume::{Component, FileKey, Query, QueryScope, TaskFile};
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn switches<const EVICT: bool>(count: usize) {
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "conversation".into(),
        };
        let key = authority_key(&owner);
        let mut session = ClientSession::open(MemoryClientState::default()).unwrap();
        session
            .views
            .transcripts
            .entry(key.clone())
            .or_default()
            .streaming_text = "archive".into();
        for index in 0..count {
            let mut replacement = ClientSession::open(MemoryClientState::default()).unwrap();
            replacement
                .views
                .transcripts
                .entry(format!("task-{index}"))
                .or_default()
                .streaming_text = "x".repeat(128);
            if EVICT {
                session.replace_task_views(replacement, &owner);
            } else {
                session
                    .views
                    .transcripts
                    .extend(replacement.views.transcripts);
            }
            assert_eq!(session.views.transcripts[&key].streaming_text, "archive");
            assert!(
                session.views.transcripts.len() <= 2,
                "unfollowed transcripts retained"
            );
        }
    }
    /// One selector of every variant, keyed to `task` where it has one. The
    /// first `GLOBAL` are global; the rest name the task.
    fn selectors(task: &str) -> Vec<Query> {
        use crate::task_volume::{ChildFile, PlanName};
        let id = Component::new(task).unwrap();
        let other = || Component::new("shared").unwrap();
        let turn = 3;
        vec![
            Query::CommandSession {
                session: other(),
                recent: 4,
            },
            Query::UiDirectory(other()),
            Query::Session(other()),
            Query::SessionFiles {
                session: other(),
                executions: true,
            },
            Query::Root,
            Query::Inputs,
            Query::Config,
            Query::Plans,
            Query::Tasks,
            Query::TaskOverviews,
            Query::ActiveAgents,
            Query::Agents,
            Query::Children,
            Query::Child(other()),
            Query::File(FileKey::Ui {
                directory: other(),
                name: other(),
            }),
            Query::File(FileKey::Goal),
            Query::File(FileKey::RequestHead),
            Query::File(FileKey::Plan(PlanName::new("plan").unwrap())),
            Query::File(FileKey::Agent(other())),
            Query::File(FileKey::Child {
                reference: other(),
                file: ChildFile::Status,
            }),
            Query::File(FileKey::Session {
                session: other(),
                file: TaskFile::Request(other()),
            }),
            Query::TaskSession {
                task: id.clone(),
                session: other(),
            },
            Query::Task(id.clone()),
            Query::TaskView {
                task: id.clone(),
                recent: 4,
                reasoning: true,
                details: true,
                terminal: true,
            },
            Query::Requests(id.clone()),
            Query::Executions(id.clone()),
            Query::Turns(id.clone()),
            Query::Turn {
                task: id.clone(),
                turn,
            },
            Query::Chunks {
                task: id.clone(),
                turn,
            },
            Query::Tools {
                task: id.clone(),
                turn,
            },
            Query::Tool {
                task: id.clone(),
                turn,
                call: other(),
            },
            Query::Approvals(id.clone()),
            Query::Notifications(id.clone()),
            Query::File(FileKey::Task {
                task: id,
                file: TaskFile::Status,
            }),
        ]
    }
    const GLOBAL: usize = 21;
    const SELECTORS: usize = 34;
    /// `selectors()[TAIL]` is the `TaskView` a switch follows.
    const TAIL: usize = GLOBAL + 2;

    fn attach(views: &mut TaskFileViews, base: &crate::task_volume::Snapshot, query: Query) {
        let mut cursor = base.cursor.clone();
        cursor.query = crate::task_volume::query_id(&query).unwrap();
        let snapshot = crate::task_volume::Snapshot {
            files: Vec::new(),
            cursor,
            generation: base.generation.clone(),
        };
        views.query_snapshot(query, snapshot, 4, 4096).unwrap();
    }

    fn base() -> crate::task_volume::Snapshot {
        use crate::task_volume::{FileValue, TaskVolume};
        use acyclic_stream::{MemoryStream, StreamPath};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let volume = TaskVolume::open(
                std::sync::Arc::new(MemoryStream::default()),
                StreamPath::new("task-switch-scope").unwrap(),
            )
            .await
            .unwrap();
            let key = FileKey::Goal;
            volume
                .append(
                    volume.head().await.unwrap(),
                    &FileValue::new(key.clone(), b"g".to_vec()),
                )
                .await
                .unwrap();
            volume.snapshot(&Query::File(key), 1).await.unwrap()
        })
    }

    /// Attaches `picks` of every task's selectors, then follows `tasks` in
    /// order. After each switch the task-scoped views are exactly the new
    /// task's tail and every global view survives. `LEGACY` restores the
    /// three-variant eviction the scope type replaced.
    fn evictions<const LEGACY: bool>(picks: &[(usize, usize)], tasks: &[usize]) {
        let base = base();
        let names = ["task-a", "task-b", "task-c"];
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "conversation".into(),
        };
        let mut session = ClientSession::open(MemoryClientState::default()).unwrap();
        for &(task, selector) in picks {
            attach(
                &mut session.views,
                &base,
                selectors(names[task])[selector].clone(),
            );
        }
        for &next in tasks {
            let globals: BTreeSet<_> = session
                .views
                .queries
                .values()
                .filter(|view| view.query().scope() == QueryScope::Global)
                .map(|view| serde_json::to_string(view.query()).unwrap())
                .collect();
            let tail = selectors(names[next])[TAIL].clone();
            let mut replacement = ClientSession::open(MemoryClientState::default()).unwrap();
            attach(&mut replacement.views, &base, tail.clone());
            if LEGACY {
                session.views.queries.retain(|_, view| {
                    !matches!(
                        view.query(),
                        Query::Tasks | Query::Task(_) | Query::TaskView { .. }
                    )
                });
                session.views.queries.extend(replacement.views.queries);
            } else {
                session.replace_task_views(replacement, &owner);
            }
            let scoped: Vec<_> = session
                .views
                .queries
                .values()
                .map(|view| view.query())
                .filter(|query| query.scope() != QueryScope::Global)
                .collect();
            assert_eq!(scoped, [&tail], "previous task selections retained");
            let after: BTreeSet<_> = session
                .views
                .queries
                .values()
                .map(|view| serde_json::to_string(view.query()).unwrap())
                .collect();
            assert!(globals.is_subset(&after), "global selection evicted");
        }
    }

    #[test]
    fn every_selector_has_the_scope_its_key_names() {
        let task = Component::new("task-a").unwrap();
        let all = selectors("task-a");
        assert_eq!(all.len(), SELECTORS);
        for (index, query) in all.iter().enumerate() {
            let expected = if index < GLOBAL {
                QueryScope::Global
            } else {
                QueryScope::Task(&task)
            };
            assert_eq!(query.scope(), expected, "{query:?}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn task_switches_evict_every_task_scoped_selection(
            picks in proptest::collection::vec((0usize..3, 0usize..SELECTORS), 0..24),
            tasks in proptest::collection::vec(0usize..3, 1..6),
        ) {
            evictions::<false>(&picks, &tasks);
        }
    }

    #[test]
    fn every_selector_of_the_previous_task_is_evicted() {
        for selector in 0..SELECTORS {
            evictions::<false>(&[(0, selector), (1, selector)], &[1, 2]);
        }
    }

    #[test]
    #[should_panic(expected = "previous task selections retained")]
    fn variant_list_eviction_is_detected_negative_control() {
        let turns = GLOBAL + 5;
        evictions::<true>(&[(0, turns)], &[1]);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]
        #[test]
        fn task_switches_retain_only_the_conversation_and_current_tail(count in 3usize..128) { switches::<true>(count); }
    }
    #[test]
    #[should_panic(expected = "unfollowed transcripts retained")]
    fn retaining_old_transcripts_is_detected_negative_control() {
        switches::<false>(3);
    }
}
