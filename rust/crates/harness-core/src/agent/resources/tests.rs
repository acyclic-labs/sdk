use super::*;
use proptest::prelude::*;

proptest! {
    // The measured transition is fixed size and allocation-free for any totals,
    // including overflow. The owned-buffer control proves the meter is active.
    #[test]
    fn resource_transition_cost_is_independent_of_totals(
        commits in any::<u64>(), records in any::<u64>(), bytes in any::<u64>(),
        new_records in any::<u64>(), new_bytes in any::<u64>(), core in any::<u32>(),
    ) {
        let counters = AgentResourceCounters {
            step_commits: commits, conversation_records: records, conversation_bytes: bytes,
        };
        let fact = ResourcePlacement {
            actor: ResourceLocation { core: Some(core), ..ResourceLocation::default() },
            ..ResourcePlacement::default()
        };
        let allocation = acyclic_test_support::measure(|| {
            let _ = std::hint::black_box(counters.step(new_records, new_bytes));
            std::hint::black_box(ObservedResourcePlacement::new(fact));
        });
        prop_assert_eq!((allocation.count_total, allocation.bytes_total), (0, 0));
        let owned = acyclic_test_support::measure(|| drop(std::hint::black_box(Box::new(counters))));
        prop_assert!(owned.count_total > 0);
        prop_assert!(matches!(AgentResourceCounters { step_commits: u64::MAX, ..counters }.step(0, 0), Err(Error::Record("agent resource counters overflow"))));
        prop_assert!(matches!(AgentResourceCounters { conversation_records: u64::MAX, ..AgentResourceCounters::default() }.step(1, 0), Err(Error::Record("agent resource counters overflow"))));
        prop_assert!(matches!(AgentResourceCounters { conversation_bytes: u64::MAX, ..AgentResourceCounters::default() }.step(0, 1), Err(Error::Record("agent resource counters overflow"))));
    }

    // Unknown and observed zero must not alias; neither may a core, NUMA node,
    // shard, actor, shell, machine, region or provider in a placement identity.
    #[test]
    fn placement_presence_and_field_position_are_bound(value in any::<u32>(), identity in any::<[u8; 32]>()) {
        let unknown = ObservedResourcePlacement::new(ResourcePlacement::default()).digest();
        let actor = ResourceLocation { core: Some(value), ..ResourceLocation::default() };
        let mut fact = ResourcePlacement { actor, ..ResourcePlacement::default() };
        let original = ObservedResourcePlacement::new(fact).digest();
        prop_assert_ne!(original, unknown);
        for location in [
            ResourceLocation { numa: Some(value), ..ResourceLocation::default() },
            ResourceLocation { shard: Some(value), ..ResourceLocation::default() },
            ResourceLocation { host: Some(identity), ..ResourceLocation::default() },
            ResourceLocation { region: Some(identity), ..ResourceLocation::default() },
            ResourceLocation { machine: Some(identity[..16].try_into().unwrap()), ..ResourceLocation::default() },
        ] {
            fact.actor = location;
            prop_assert_ne!(original, ObservedResourcePlacement::new(fact).digest());
        }
        fact.actor = ResourceLocation::default();
        fact.shell = actor;
        prop_assert_ne!(original, ObservedResourcePlacement::new(fact).digest());
        fact = ResourcePlacement { filesystem_provider: Some(identity), ..ResourcePlacement::default() };
        prop_assert_ne!(unknown, ObservedResourcePlacement::new(fact).digest());
    }
}

#[cfg(feature = "research")]
mod committed {
    use super::*;
    use crate::agent::{
        AgentPath, AgentRecord, AgentState, ForkAdopted, InboxRecord, IntentId, Out, Queued,
        TurnDriver, TurnFuture, TurnStep, Wait, Wake, delivery_key, lower,
    };
    use acyclic_actors::guest::CancellationToken;
    use acyclic_stream::{
        CommitCondition, CommitOutcome, MemoryStream, ReadRequest, StreamProvider,
    };
    use futures::TryStreamExt;

    // A model-free control driver. The bytes are generated internal conversation
    // entries, not fabricated transcripts from an external tool or provider.
    struct Conversation(Vec<u8>);
    impl TurnDriver for Conversation {
        fn step<'a>(
            &'a self,
            _: &'a AgentState,
            _: u64,
            _: Option<Queued>,
            _: CancellationToken,
        ) -> TurnFuture<'a> {
            Box::pin(async move {
                Ok(TurnStep {
                    checkpoint: None,
                    reserved_children: 0,
                    outs: vec![Out::Conversation(self.0.clone())],
                    wait: Wait::Input,
                    drops: None,
                })
            })
        }
    }

    fn adoption(agent: AgentId) -> ForkAdopted {
        ForkAdopted {
            profile: None,
            agent,
            parent: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: Vec::new(),
            brief: None,
            policy: None,
        }
    }

    async fn snapshots(
        provider: &MemoryStream,
        agent: &AgentId,
    ) -> Result<Vec<AgentResourceSnapshot>, Box<dyn std::error::Error>> {
        let records: Vec<_> = provider
            .read(ReadRequest {
                key: None,
                path: resource_stream(agent)?,
                from: 0,
                limit: 16,
                at_least: Default::default(),
            })
            .await?
            .try_collect()
            .await?;
        records
            .into_iter()
            .map(|record| AgentResourceSnapshot::decode(&record.value).map_err(Into::into))
            .collect()
    }

    #[tokio::test]
    async fn resource_snapshot_replays_atomically_and_refuses_a_second_step()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = MemoryStream::default();
        let agent = AgentId::from_bytes([1; 16]);
        let path = AgentPath::root(&agent);
        let created =
            crate::agent::create_root(&IntentId::try_from("root")?, adoption(agent.clone()))?;
        assert!(matches!(
            provider.commit(created.request).await?,
            CommitOutcome::Committed(_)
        ));
        let placement = ObservedResourcePlacement::new(ResourcePlacement {
            actor: ResourceLocation {
                core: Some(0),
                ..ResourceLocation::default()
            },
            ..ResourcePlacement::default()
        });
        let prepared = crate::agent::machine::prepare_step(
            &Conversation(Vec::new()),
            &path,
            None,
            1,
            Wake::Inbox {
                sequence: 0,
                record: InboxRecord::ForkAdopted(Box::new(adoption(agent.clone()))),
            },
            CancellationToken::new(),
            Some(&placement),
        )
        .await?;
        let step = prepared.step();
        let request = lower(step, 1, delivery_key(&step.state, "record:0")?)?;
        let state = step.state.clone();
        let checkpoint = prepared.into_checkpoint();
        let CommitOutcome::Committed(first) = provider.commit(request.clone()).await? else {
            panic!("first step refused")
        };
        let CommitOutcome::Committed(replay) = provider.commit(request.clone()).await? else {
            panic!("replay refused")
        };
        assert_eq!(first.commit_id, replay.commit_id);
        let rows = snapshots(&provider, &agent).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].state,
            crate::agent::checkpoint::checkpoint_state(&checkpoint)?
                .resources
                .unwrap()
        );
        assert_eq!(rows[0].state.counters.step_commits, 1);
        assert_eq!(rows[0].placement, Some(placement.fact()));

        let next = crate::agent::machine::prepare_step(
            &Conversation(Vec::new()),
            &path,
            Some(state),
            1,
            Wake::Alarm,
            CancellationToken::new(),
            Some(&placement),
        )
        .await?;
        assert_eq!(next.step().resource_placement, None);
        let next = lower(next.step(), 1, delivery_key(&next.step().state, "alarm:1")?)?;
        assert!(matches!(
            provider.commit(next.clone()).await?,
            CommitOutcome::Committed(_)
        ));
        // With no conversation/status append on an idle alarm, this resource
        // guard alone prevents a stale checkpoint from adding a second row.
        assert_eq!(next.mutations.len(), 1);
        let mut stale = next;
        stale.idempotency_key = crate::agent::intent_key("stale", &[])?;
        assert!(matches!(
            provider.commit(stale.clone()).await?,
            CommitOutcome::Conflict(_)
        ));
        // Negative control: removing the original resource cut admits duplication.
        stale.conditions.retain(|condition| !matches!(condition, CommitCondition::Tail { path, .. } if path == &resource_stream(&agent).unwrap()));
        assert!(matches!(
            provider.commit(stale).await?,
            CommitOutcome::Committed(_)
        ));
        let rows = snapshots(&provider, &agent).await?;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1], rows[2]);

        let mut malformed = rows[0];
        malformed.placement.as_mut().unwrap().actor.core = Some(1);
        assert!(AgentResourceSnapshot::decode(&serde_json::to_vec(&malformed)?).is_err());
        malformed = rows[0];
        malformed.version = 2;
        assert!(AgentResourceSnapshot::decode(&serde_json::to_vec(&malformed)?).is_err());
        malformed = rows[0];
        malformed.state.counters.step_commits = 0;
        assert!(AgentResourceSnapshot::decode(&serde_json::to_vec(&malformed)?).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn inherited_conversation_is_shared_without_inherited_resource_charges()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::agent::{Approval, Phase, Progress, Sender};
        let provider = MemoryStream::default();
        let agent = AgentId::from_bytes([2; 16]);
        let created =
            crate::agent::create_root(&IntentId::try_from("root")?, adoption(agent.clone()))?;
        let parent = created.child;
        assert!(matches!(
            provider.commit(created.request).await?,
            CommitOutcome::Committed(_)
        ));
        let mut state = AgentState::adopted(parent.clone(), &adoption(agent.clone()))?;
        // Resume a generated internal turn to append an actual conversation
        // record through production preparation and the real Streams commit.
        state.phase = Phase::Running;
        state.turn = Some(Progress {
            inbox: 0,
            checkpoint: Vec::new(),
        });
        let prepared = crate::agent::machine::prepare_step(
            &Conversation(vec![7; 257]),
            &parent,
            Some(state),
            1,
            Wake::Alarm,
            CancellationToken::new(),
            None,
        )
        .await?;
        let step = prepared.step();
        let counters = step.state.resources.unwrap().counters;
        assert_eq!(counters.conversation_records, 1);
        assert_eq!(counters.conversation_bytes, 257);
        let request = lower(step, 1, delivery_key(&step.state, "parent-output")?)?;
        assert!(matches!(
            provider.commit(request).await?,
            CommitOutcome::Committed(_)
        ));

        let mut adopted = adoption(agent);
        adopted.conversation_tail = 1;
        let child = crate::agent::spawn(&parent, &IntentId::try_from("child")?, 1, adopted)?;
        let child_path = child.child;
        assert!(matches!(
            provider.commit(child.request).await?,
            CommitOutcome::Committed(_)
        ));
        // The child's original adoption is appended after its inherited inbox.
        let records: Vec<_> = provider
            .read(ReadRequest {
                key: None,
                path: child_path.inbox()?,
                from: 1,
                limit: 1,
                at_least: Default::default(),
            })
            .await?
            .try_collect()
            .await?;
        let InboxRecord::ForkAdopted(adopted) = InboxRecord::decode(&records[0].value)? else {
            panic!("missing adoption")
        };
        let adopted_state = AgentState::adopted(child_path.clone(), &adopted)?;
        assert_eq!(adopted_state.conversation_tail, 1);
        assert_eq!(adopted_state.resources, None);
        let child_step = crate::agent::machine::prepare_step(
            &Conversation(Vec::new()),
            &child_path,
            None,
            2,
            Wake::Inbox {
                sequence: 1,
                record: InboxRecord::ForkAdopted(adopted),
            },
            CancellationToken::new(),
            None,
        )
        .await?;
        let step = child_step.step();
        assert_eq!(
            step.state.resources.unwrap().counters,
            AgentResourceCounters {
                step_commits: 1,
                ..AgentResourceCounters::default()
            }
        );
        assert_eq!(step.state.conversation_tail, 1);
        let child_resource = resource_stream(&child_path.id())?;
        assert!(!child_resource.as_str().starts_with(parent.as_str()));
        assert!(matches!(
            provider
                .commit(lower(
                    step,
                    2,
                    delivery_key(&step.state, "child-adoption")?
                )?)
                .await?,
            CommitOutcome::Committed(_)
        ));
        let child_rows = snapshots(&provider, &child_path.id()).await?;
        assert_eq!(child_rows.len(), 1);
        assert_eq!(child_rows[0].state.counters.conversation_bytes, 0);
        assert_eq!(
            snapshots(&provider, &parent.id()).await?[0]
                .state
                .counters
                .conversation_bytes,
            257
        );

        // Negative control: treating inherited tail as work would charge the
        // child, although it appended no conversation record.
        assert_ne!(
            child_rows[0].state.counters.conversation_records,
            step.state.conversation_tail
        );
        // An input append alone does not change counters outside a step commit.
        let input = InboxRecord::Approval(Approval {
            id: IntentId::try_from("next")?,
        });
        assert!(matches!(
            provider
                .commit(crate::agent::send(
                    &child_path,
                    &Sender::User {
                        principal: "research".into()
                    },
                    &input
                )?)
                .await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(snapshots(&provider, &child_path.id()).await?, child_rows);
        Ok(())
    }

    #[tokio::test]
    async fn discarded_checkpoint_preparation_counts_one_step_and_preserves_a_move()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::agent::{Phase, Progress};
        struct Oversized;
        impl TurnDriver for Oversized {
            fn step<'a>(
                &'a self,
                _: &'a AgentState,
                _: u64,
                _: Option<Queued>,
                _: CancellationToken,
            ) -> TurnFuture<'a> {
                Box::pin(async {
                    Ok(TurnStep {
                        checkpoint: Some(vec![0; crate::agent::MAX_CHECKPOINT_BYTES]),
                        reserved_children: 0,
                        outs: Vec::new(),
                        wait: Wait::Continue,
                        drops: None,
                    })
                })
            }
        }
        let agent = AgentId::from_bytes([3; 16]);
        let path = AgentPath::root(&agent);
        let before = ObservedResourcePlacement::new(ResourcePlacement::default());
        let after = ObservedResourcePlacement::new(ResourcePlacement {
            actor: ResourceLocation {
                shard: Some(1),
                ..ResourceLocation::default()
            },
            ..ResourcePlacement::default()
        });
        let mut state = AgentState::adopted(path.clone(), &adoption(agent))?;
        state.phase = Phase::Running;
        state.turn = Some(Progress {
            inbox: 0,
            checkpoint: Vec::new(),
        });
        state.resources = Some(AgentResourceState {
            counters: AgentResourceCounters {
                step_commits: 7,
                conversation_records: 5,
                conversation_bytes: 11,
            },
            placement: Some(before.digest()),
        });
        let prepared = crate::agent::machine::prepare_step(
            &Oversized,
            &path,
            Some(state),
            1,
            Wake::Alarm,
            CancellationToken::new(),
            Some(&after),
        )
        .await?;
        let step = prepared.step();
        assert!(matches!(step.state.phase, Phase::Done { .. }));
        assert!(step.state.turn.is_none());
        assert_eq!(
            step.state.resources.unwrap().counters,
            AgentResourceCounters {
                step_commits: 8,
                conversation_records: 5,
                conversation_bytes: 11,
            }
        );
        assert_eq!(step.resource_placement, Some(after.fact()));
        assert_eq!(
            step.state.resources.unwrap().placement,
            Some(after.digest())
        );
        let checkpoint = prepared.into_checkpoint();
        let state = crate::agent::checkpoint::checkpoint_state(&checkpoint)?;
        let cleared = crate::agent::machine::prepare_step(
            &Conversation(Vec::new()),
            &path,
            Some(state),
            1,
            Wake::Alarm,
            CancellationToken::new(),
            None,
        )
        .await?;
        assert_eq!(cleared.step().state.resources.unwrap().placement, None);
        assert_eq!(cleared.step().resource_placement, None);
        assert_eq!(
            cleared
                .step()
                .state
                .resources
                .unwrap()
                .counters
                .step_commits,
            9
        );
        Ok(())
    }
}
