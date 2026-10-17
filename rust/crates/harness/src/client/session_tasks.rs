use super::*;

impl<S: ClientStateStore> ClientSession<S> {
    pub(in crate::client) fn replace_task_views(
        &mut self,
        replacement: Self,
        owner: &wire::Authority,
    ) {
        let owner = authority_key(owner);
        self.views.transcripts.retain(|key, _| key == &owner);
        // Only the task tail is replaced; other shared folds (task overviews)
        // keep following across task switches.
        self.views.queries.retain(|_, view| {
            use crate::task_volume::Query;
            !matches!(
                view.query(),
                Query::Tasks | Query::Task(_) | Query::TaskView { .. }
            )
        });
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
    use proptest::prelude::*;

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
