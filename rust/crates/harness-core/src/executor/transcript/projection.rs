//! Projection metadata refers to the same immutable segments used by dispatch.
use super::*;

#[cfg(all(test, not(target_arch = "wasm32"), not(loom)))]
mod tests;

impl SteeringPrefix {
    async fn load(
        self,
        journal: &dyn ExecutionJournal,
        io: Option<&PreparationIo>,
    ) -> Result<crate::agent::SteeringSnapshot> {
        Ok(crate::agent::SteeringSnapshot {
            through: self.through,
            messages: match self.messages {
                Some(prefix) => load_messages(journal, prefix, io).await?,
                None => Vec::new(),
            },
        })
    }
}

impl ProjectionMetadata {
    async fn load(
        self,
        journal: &dyn ExecutionJournal,
        io: Option<&PreparationIo>,
    ) -> Result<ResponseProjection> {
        Ok(ResponseProjection {
            context: crate::context::Context {
                messages: Vec::new(),
                metadata: self.context.metadata,
                current_input_index: self.context.current_input_index,
            },
            steering: match self.steering {
                Some(steering) => Some(steering.load(journal, io).await?),
                None => None,
            },
            canonical: self.canonical,
            checkpoint: self.checkpoint,
            working: self.working,
            accounting: self.accounting,
        })
    }
}

impl ResponseProjection {
    /// Recover the admitted steering without imposing a new context policy.
    pub(crate) async fn steering_snapshot(
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
    ) -> Result<Option<crate::agent::SteeringSnapshot>> {
        match load_projection_record(journal, reference, None)
            .await?
            .projection
            .steering
        {
            Some(steering) => Ok(Some(steering.load(journal, None).await?)),
            None => Ok(None),
        }
    }

    pub(in crate::executor) async fn continuation_metadata(
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
        original: Option<&PreparationIo>,
    ) -> Result<Self> {
        let scope = crate::filesystem::PreparationScope::new(original)?;
        let io = scope.io();
        let stored = load_projection_record(journal, reference, io)
            .await?
            .projection;
        let projection = Self {
            context: crate::context::Context {
                messages: Vec::new(),
                metadata: stored.context.metadata,
                current_input_index: stored.context.current_input_index,
            },
            steering: None,
            canonical: None,
            checkpoint: None,
            working: stored.working,
            accounting: None,
        };
        drop((
            stored.steering,
            stored.canonical,
            stored.checkpoint,
            stored.accounting,
        ));
        {
            let retained = projection
                .decoded_owned_bytes()?
                .ok_or_else(|| invalid(io))?;
            scope.finish(retained)?;
        }
        Ok(projection)
    }
}

pub(crate) async fn load_projection<'a>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    original: Option<&'a PreparationIo>,
) -> Result<(
    ResponseProjection,
    Option<crate::conversation::ContentHold<'a>>,
)> {
    let scope = crate::filesystem::PreparationScope::new(original)?;
    let io = scope.io();
    let stored = load_projection_record(journal, reference, io).await?;
    let request = load_request(journal, &stored.request, limits, io).await?;
    let mut projection = stored.projection.load(journal, io).await?;
    projection.context.messages = request.into_messages();

    let retained = projection
        .decoded_owned_bytes()?
        .ok_or_else(|| invalid(io))?;
    crate::context::validate_projected_context(&projection.context, limits, io)?;
    let hold = {
        scope.finish(retained)?;
        original.map(|io| io.guard_owned(retained)).transpose()?
    };
    Ok((projection, hold))
}

impl StockExecution<'_> {
    pub(in crate::executor) async fn load_response_projection(
        &self,
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
    ) -> Result<(ResponseProjection, crate::model::PreparedModelRequest)> {
        let scope = crate::filesystem::PreparationScope::new(self.io)?;

        let io = scope.io();
        let stored = load_projection_record(journal, reference, io).await?;
        let prepared = load_request(journal, &stored.request, self.stock.limits, io).await?;
        let projection = stored.projection.load(journal, io).await?;
        crate::context::validate_projected_request(&projection.context, &prepared, io)?;

        {
            let retained = crate::core::owned_sum([
                projection
                    .decoded_owned_bytes()?
                    .ok_or_else(|| invalid(io))?,
                io.map_or(Ok(0), |io| prepared.preparation_owned_bytes(io))?,
            ])?;
            scope.finish(retained)?;
        }
        Ok((projection, prepared))
    }
}
