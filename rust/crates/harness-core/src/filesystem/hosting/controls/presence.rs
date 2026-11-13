//! Public display fields are derived from committed, keyed filesystem events.
use super::*;
use crate::agent::SessionPublication;

// One public display chunk; the authored goal remains unrestricted.
const GOAL_PREVIEW_BYTES: u64 = 256;

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Controls<P, O> {
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
        let access = ContentAccess {
            ceiling: Some(ceiling),
            io: None,
        };
        let changed = |path: &[u8]| {
            record
                .headers
                .keys()
                .binary_search_by(|key| key.as_ref().cmp(path))
                .is_ok()
        };
        let volume = selected.volume()?;
        if selected.as_volume() == state.private()?.as_volume() && changed(b"ui/title") {
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
        if state
            .home()
            .is_some_and(|home| home.as_volume() == selected.as_volume())
            && changed(b"GOAL.md")
        {
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
}
