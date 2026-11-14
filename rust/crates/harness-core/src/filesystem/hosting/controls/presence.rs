//! Public display fields are derived from committed, keyed filesystem events.
use super::*;
use crate::agent::SessionPublication;

// One public display chunk; the authored goal remains unrestricted.
const GOAL_PREVIEW_BYTES: u64 = 256;

struct DisplayFields {
    title: bool,
    goal: bool,
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Controls<P, O> {
    pub(super) async fn membership_transition(
        &self,
        delivery: &Delivery,
        mut state: State,
    ) -> Result<Transition, Error> {
        use crate::agent::{AgentRecord, PeerMembership, SessionRow};
        use acyclic_actors::guest::SubscriptionChange;
        use acyclic_stream::{HeadProjection, HeadsRequest, RetainedRecord};
        let routing = self
            .routing
            .as_ref()
            .ok_or(Error::Record("metadata has no routing context"))?;
        let record = delivery
            .record
            .as_ref()
            .ok_or(Error::Record("membership input missing"))?;
        let membership = state.agent.peer_membership()?;
        if state.agent.depth() != 0
            || record.origin != membership
            || delivery
                .subscriptions
                .get(PEERS)
                .is_none_or(|cut| cut.path != membership)
        {
            return Err(Error::Record("metadata membership source differs"));
        }
        PeerMembership::decode(&record.value)?;
        state.metadata_private_from = None;
        state.metadata_home_from = None;
        let mut subscriptions = [TITLE, GOAL]
            .map(|id| SubscriptionChange::Remove { id: id.into() })
            .to_vec();
        subscriptions.push(SubscriptionChange::Remove { id: PEERS.into() });
        let actor = ActorReference::new(
            delivery.actor.account().as_bytes(),
            delivery.actor.namespace().as_str().as_bytes(),
            state.agent.id().as_str().as_bytes(),
        )
        .map_err(|_| Error::Record("metadata target identity"))?;
        let row_path = SessionPublication::row_path(&actor)?;
        let private_path = self
            .source(State::private(&state.sources)?.volume()?)
            .map_err(turn)?;
        let home_path = State::home(&state.sources)
            .map(|entry| self.source(entry.volume()?).map_err(turn))
            .transpose()?;
        let mut paths = vec![row_path.clone(), private_path.clone()];
        if let Some(home) = &home_path
            && home != &private_path
        {
            paths.push(home.clone());
        }
        let membership_index = paths.len();
        paths.push(membership.clone());
        let request = HeadsRequest {
            retained: [RetainedRecord {
                commit_id: record.commit_id,
                path: membership.clone(),
                sequence: record.sequence,
            }]
            .into_iter()
            .collect(),
            durable_through: None,
            projection: HeadProjection::History,
            limit: u32::try_from(paths.len() + 1)
                .map_err(|_| Error::Record("metadata source count overflow"))?,
            paths: paths.into_iter().collect(),
            children: None,
            at_least: Default::default(),
        };
        let view = self
            .store
            .provider()
            .heads(request.clone())
            .await
            .map_err(Error::Stream)?;
        view.validate_for(&request).map_err(Error::Stream)?;
        if view
            .retained
            .first()
            .and_then(|observed| observed.record.as_ref())
            != Some(record)
        {
            return Err(Error::Record(
                "membership event is outside the metadata cut",
            ));
        }
        let member_head = view.heads[membership_index]
            .as_ref()
            .ok_or(Error::Record("membership head missing"))?;
        let member = member_head
            .last
            .as_ref()
            .ok_or(Error::Record("membership head empty"))?;
        if member.origin != membership || member.sequence.checked_add(1) != Some(member_head.tail) {
            return Err(Error::Record("membership head provenance differs"));
        }
        let enabled = PeerMembership::decode(&member.value)?.enabled();
        // Membership is a current boolean projection. Skip already superseded
        // decisions at this same cut, rather than replaying their file reads.
        subscriptions.push(SubscriptionChange::Add {
            id: PEERS.into(),
            path: membership,
            from: Some(member_head.tail),
        });
        if !enabled {
            let mut transition = state.transition(Vec::new(), false)?;
            transition.subscriptions = subscriptions;
            return Ok(transition);
        }
        let head = view.heads[0]
            .as_ref()
            .ok_or(Error::Record("member row missing"))?;
        let row = SessionRow::from_record(
            &row_path,
            head.tail,
            head.last
                .as_ref()
                .ok_or(Error::Record("member row empty"))?,
        )?;
        if row.node != routing.node {
            return Err(Error::Record("metadata row node differs"));
        }
        let Some(mut profile) = row.profile.clone() else {
            let mut transition = state.transition(Vec::new(), false)?;
            transition.subscriptions = subscriptions;
            return Ok(transition);
        };
        let authority = agent_authority(
            &self.issuer,
            &self.admission,
            &state.agent,
            state.sources.entries(),
            &[],
        )
        .await
        .map_err(turn)?;
        for selected in state.sources.entries() {
            let source = self.source(selected.volume()?).map_err(turn)?;
            if selected.role == crate::agent::WorkspaceRole::Home && source == private_path {
                continue;
            }
            let index = if source == private_path { 1 } else { 2 };
            let head = view.heads[index]
                .as_ref()
                .ok_or(Error::Record("metadata volume has no source"))?;
            let original = head
                .last
                .as_ref()
                .ok_or(Error::Record("metadata source is empty"))?;
            if original.origin != source || original.sequence.checked_add(1) != Some(head.tail) {
                return Err(Error::Record("metadata source provenance differs"));
            }
            let context = acyclic_stream::changes_context(&original.value, &original.headers)
                .map_err(Error::Stream)?
                .ok_or(Error::Record("metadata source has no generation"))?;
            let context =
                acyclic_fs::PublicationContext::from_bytes(context).map_err(Error::Stream)?;
            if acyclic_fs::session::changes_path(&self.volumes.filesystem, context.volume)
                .map_err(Error::Stream)?
                != source
            {
                return Err(Error::Record("metadata source volume differs"));
            }
            let generation = self
                .volumes
                .generation_ref_id(context.generation, None)
                .map_err(turn)?;
            let grant = crate::conversation::ContentGrant::verify_directory_read(
                &authority.verifier,
                &authority.scope,
                selected.volume()?,
                "",
                &mut None,
            )
            .map_err(turn)?;
            self.project_display(
                selected.volume()?,
                &generation,
                &mut profile,
                &grant,
                ContentAccess {
                    ceiling: Some(authority.scope.capabilities()),
                    io: None,
                },
                DisplayFields {
                    title: source == private_path,
                    goal: home_path.as_ref() == Some(&source),
                },
            )
            .await?;
        }
        let private_tail = view.heads[1]
            .as_ref()
            .ok_or(Error::Record("private source missing"))?
            .tail;
        state.metadata_private_from = Some(private_tail);
        subscriptions.push(SubscriptionChange::Add {
            id: TITLE.into(),
            path: private_path.clone(),
            from: Some(private_tail),
        });
        if let Some(home) = home_path {
            let index = if home == private_path { 1 } else { 2 };
            let tail = view.heads[index]
                .as_ref()
                .ok_or(Error::Record("home source missing"))?
                .tail;
            state.metadata_home_from = Some(tail);
            subscriptions.push(SubscriptionChange::Add {
                id: GOAL.into(),
                path: home,
                from: Some(tail),
            });
        }
        let effects = if let Some(row) = row.changed(Some(profile))? {
            SessionPublication {
                actor,
                row,
                intent: crate::agent::intent_key(
                    "session-metadata-snapshot",
                    &[
                        &hex::encode(record.commit_id.as_bytes()),
                        &record.sequence.to_string(),
                    ],
                )?,
            }
            .effects(&routing.paths)?
            .into()
        } else {
            Vec::new()
        };
        let mut transition = state.transition(effects, false)?;
        transition.subscriptions = subscriptions;
        Ok(transition)
    }

    pub(super) async fn metadata_effects(
        &self,
        delivery: &Delivery,
        state: &State,
        selected: &WorkspaceEntry,
        generation: &crate::resources::GenerationRef,
        grant: &crate::conversation::ContentGrant,
        ceiling: &crate::Capabilities,
    ) -> Result<Vec<CommitEffect>, Error> {
        let Some(routing) = &self.routing else {
            return Ok(Vec::new());
        };
        if state.agent.depth() != 0 {
            return Ok(Vec::new());
        }
        let record = delivery
            .record
            .as_ref()
            .ok_or(Error::Record("metadata source missing"))?;
        let within = &delivery.actor;
        let actor = ActorReference::new(
            within.account().as_bytes(),
            within.namespace().as_str().as_bytes(),
            state.agent.id().as_str().as_bytes(),
        )
        .map_err(|_| Error::Record("metadata target identity"))?;
        let (row, _) =
            SessionPublication::observe(self.store.provider().as_ref(), &actor, &routing.node)
                .await?;
        let Some(row) = row else {
            return Ok(Vec::new());
        };
        let Some(mut profile) = row.profile.clone() else {
            return Ok(Vec::new());
        };
        let changed = |path: &[u8]| {
            record
                .headers
                .keys()
                .binary_search_by(|key| key.as_ref().cmp(path))
                .is_ok()
        };
        let fields = DisplayFields {
            title: selected.as_volume() == State::private(&state.sources)?.as_volume()
                && changed(b"ui/title"),
            goal: State::home(&state.sources)
                .is_some_and(|home| home.as_volume() == selected.as_volume())
                && changed(b"GOAL.md"),
        };
        self.project_display(
            selected.volume()?,
            generation,
            &mut profile,
            grant,
            ContentAccess {
                ceiling: Some(ceiling),
                io: None,
            },
            fields,
        )
        .await?;
        let Some(row) = row.changed(Some(profile))? else {
            return Ok(Vec::new());
        };
        let commit = hex::encode(record.commit_id.as_bytes());
        let sequence = record.sequence.to_string();
        let publication = SessionPublication {
            actor,
            row,
            intent: crate::agent::intent_key("session-file-metadata", &[&commit, &sequence])?,
        };
        // The row and ordered node outbox share the companion's checkpoint;
        // restart replays the original file event rather than scanning files.
        Ok(publication.effects(&routing.paths)?.into())
    }
    async fn project_display(
        &self,
        volume: &VolumeRef,
        generation: &crate::resources::GenerationRef,
        profile: &mut crate::agent::SessionProfile,
        grant: &crate::conversation::ContentGrant,
        access: ContentAccess<'_, '_>,
        fields: DisplayFields,
    ) -> Result<(), Error> {
        if fields.title {
            let title = self
                .volumes
                .read_private_path_indexed(
                    volume,
                    grant,
                    "ui/title",
                    Some(generation),
                    self.maximum_bytes,
                    access,
                )
                .await;
            profile.title = match title {
                Ok(selected) => selected.map_or_else(String::new, |(_, bytes, _)| {
                    String::from_utf8_lossy(&bytes).into_owned()
                }),
                // Malformed display input must not prevent a pause/stop change
                // carried by this same original filesystem publication.
                Err(crate::Error::ToolRejected {
                    code: crate::wire::ErrorCode::Invalid,
                    ..
                }) => String::new(),
                Err(error) => return Err(turn(error)),
            };
        }
        if fields.goal {
            let goal = self
                .volumes
                .read_private_range(
                    volume,
                    grant,
                    "GOAL.md",
                    Some(generation),
                    acyclic_fs::ByteRange {
                        offset: 0,
                        length: GOAL_PREVIEW_BYTES,
                    },
                    access,
                )
                .await;
            profile.goal_preview = match goal {
                Ok((bytes, _, _)) => {
                    let end = std::str::from_utf8(&bytes)
                        .err()
                        .map_or(bytes.len(), |error| error.valid_up_to());
                    std::str::from_utf8(&bytes[..end])
                        .map_err(|_| Error::Record("goal preview is not UTF-8"))?
                        .to_owned()
                }
                Err(crate::Error::ToolRejected {
                    code: crate::wire::ErrorCode::Invalid | crate::wire::ErrorCode::NotFound,
                    ..
                }) => String::new(),
                Err(error) => return Err(turn(error)),
            };
        }
        Ok(())
    }
}
