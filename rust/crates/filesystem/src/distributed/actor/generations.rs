//! Locator coverage follows successful authority creation, not lineage ancestry.
//! A fork inherits a lineage prefix but creates only its own source locator.
//! Cold activation has no catalog: missing knowledge never certifies absence.
use super::*;
use crate::kernel::allocation::fixed_index::FixedIndex;
use crate::kernel::allocation::retained::RetainedArc;

pub(super) struct GenerationIdentity(RetainedArc<std::sync::OnceLock<acyclic_stream::CommitId>>);

impl GenerationIdentity {
    fn new(work: &mut AuthorityWork<'_>) -> Result<Self, AuthorityStoreError> {
        RetainedArc::new(std::sync::OnceLock::new(), &mut work.accounting())
            .map(Self)
            .map_err(authority_error)
    }

    pub(super) fn seal(self, commit_id: acyclic_stream::CommitId) {
        self.0
            .set(commit_id)
            .unwrap_or_else(|_| unreachable!("one prepared batch has one durable identity"));
    }
}

struct GenerationFact {
    tail: u64,
    identity: RetainedArc<std::sync::OnceLock<acyclic_stream::CommitId>>,
}

pub(super) struct GenerationCatalog {
    facts: std::sync::Mutex<FixedIndex<GenerationFact, { ObjectId::ENCODED_LEN }>>,
}

pub(in crate::distributed) enum GenerationKnowledge {
    Unknown,
    Absent,
    Present(acyclic_stream::CommitId),
}

#[derive(Clone, Copy)]
pub(in crate::distributed) struct GenerationSeeds {
    pub(in crate::distributed) inherited: Option<(GenerationId, u64)>,
    pub(in crate::distributed) published: Option<GenerationId>,
}

impl Projection {
    pub(in crate::distributed) fn generation(
        &self,
        generation: GenerationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<GenerationKnowledge, AuthorityStoreError> {
        let Some(catalog) = &self.generations else {
            work.check()?;
            return Ok(GenerationKnowledge::Unknown);
        };
        let facts = catalog
            .facts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let fact = facts
            .get(generation.object_id().index_key(), &mut work.accounting())
            .map_err(authority_error)?;
        Ok(match fact {
            None => GenerationKnowledge::Absent,
            // A shared catalog may contain a prepared successor. Its entries
            // cannot widen an earlier projection's verified lineage frontier.
            Some(fact) if fact.tail > self.lineage_tail => GenerationKnowledge::Absent,
            Some(fact) => match fact.identity.get() {
                Some(identity) => GenerationKnowledge::Present(*identity),
                None => GenerationKnowledge::Unknown,
            },
        })
    }
}

impl GenerationCatalog {
    pub(super) fn creation(
        inherited: Option<(GenerationId, u64)>,
        generation: Option<(GenerationId, u64)>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(RetainedArc<Self>, GenerationIdentity), AuthorityStoreError> {
        let catalog = RetainedArc::new(
            Self {
                facts: std::sync::Mutex::new(FixedIndex::default()),
            },
            &mut work.accounting(),
        )
        .map_err(authority_error)?;
        let identity = GenerationIdentity::new(work)?;
        for (generation, tail) in inherited.into_iter().chain(generation) {
            catalog.stage(generation, tail, &identity, work)?;
        }
        Ok((catalog, identity))
    }

    pub(super) fn prepare(
        &self,
        start: u64,
        rows: &[acyclic_stream::AppendRecord],
        work: &mut AuthorityWork<'_>,
    ) -> Result<GenerationIdentity, AuthorityStoreError> {
        let identity = GenerationIdentity::new(work)?;
        for (offset, row) in rows.iter().enumerate() {
            let generation = decode_lineage_generation(&row.value)?;
            let tail = start
                .checked_add(crate::foundation::usize_to_u64(offset))
                .and_then(|value| value.checked_add(1))
                .ok_or(crate::WorkError::Overflow)?;
            self.stage(generation, tail, &identity, work)?;
        }
        Ok(identity)
    }

    fn stage(
        &self,
        generation: GenerationId,
        tail: u64,
        identity: &GenerationIdentity,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        if tail == 0 {
            return Err(authority_corrupt(
                "catalog locator selects an empty lineage",
                work,
            ));
        }
        let mut facts = self
            .facts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let fact = facts
            .get_or_try_insert_with(
                generation.object_id().index_key(),
                &mut work.accounting(),
                |_| {
                    Ok(GenerationFact {
                        tail,
                        identity: identity.0.clone(),
                    })
                },
            )
            .map_err(authority_error)?;
        if fact.identity.get().is_some() {
            return Err(authority_corrupt(
                "catalog generation was appended twice",
                work,
            ));
        }
        // Failed preparations leave only unsealed facts. Replacing one neither
        // changes an older frontier nor promotes a failed attempt to durable.
        *fact = GenerationFact {
            tail,
            identity: identity.0.clone(),
        };
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
