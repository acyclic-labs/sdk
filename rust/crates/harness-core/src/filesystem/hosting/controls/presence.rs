//! Public display fields are derived from committed, keyed filesystem events.
use super::*;
use crate::agent::SessionPublication;

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Controls<P, O> {
    async fn metadata_effects(
        &self,
        delivery: &Delivery,
        state: &State,
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
        profile.title = self
            .volumes
            .read_private_path_indexed(
                state.private.volume()?,
                grant,
                "ui/title",
                Some(generation),
                self.maximum_bytes,
                access,
            )
            .await
            .map_err(turn)?
            .map_or_else(String::new, |(_, bytes, _)| {
                String::from_utf8_lossy(&bytes).into_owned()
            });
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
