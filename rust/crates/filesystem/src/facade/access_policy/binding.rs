use super::*;
use crate::access::{
    AccessBinding, AuthenticatedFacts, Generations, InvalidPolicy, PathTrie, Resource,
};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Load the authenticated policy revision, select generation facts, and
    /// compile a caller view through the shared metered policy program. Owned
    /// output custody covers only this binding, never the cached policy bytes.
    pub async fn compile_access<W: FsWork>(
        &mut self,
        binding: AccessBinding<'_>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<PathTrie> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .bind_access_accounted(binding, &mut accounting, cancellation)
            .await;
        work.finish_owned(result, &accounting)
    }

    async fn bind_access_accounted(
        &mut self,
        binding: AccessBinding<'_>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(PathTrie, KernelOwnedAllocation), FsError> {
        let revision = self
            .read_metadata_with_accounting(self.root.root_file_id, accounting, cancellation)
            .await?
            .named_attributes;
        let policy = self
            .compiled_access_policy_accounted(accounting, cancellation)
            .await?;
        accounting.charge_items(1)?;
        let facts_supported = match binding.facts {
            AuthenticatedFacts::Identity => policy.uses_only_authenticated_identity(),
            AuthenticatedFacts::MetadataStream => policy.permits_metadata_stream(),
            AuthenticatedFacts::Coarse => policy.permits_coarse_binding(),
        };
        if !facts_supported {
            return Err(FsError::InvalidAccessPolicy);
        }
        let (generation, head) = self
            .access_generation_facts(&binding, &policy, accounting, cancellation)
            .await?;
        let mut volume_hold = accounting.claim_owned(36, 1)?;
        let volume = uuid::Uuid::from_bytes(self.volume_id().into_bytes()).to_string();
        accounting.charge(WorkCounters {
            bytes_copied: 36,
            ..WorkCounters::UNCHARGED
        })?;
        let mut stages = [None, None, None, None];
        let mut stage = 0;
        let mut admission_error = None;
        let mut remaining =
            usize::try_from(accounting.remaining()?.items_examined).unwrap_or(usize::MAX);
        let before = remaining;
        let compiled = policy.bind_with_work(
            binding.principal,
            Resource {
                config: self.volume.config,
                lineage: &[],
                volume: &volume,
                generation,
                head,
                path: "",
            },
            binding.context,
            binding.capability,
            &mut remaining,
            |bytes| match accounting.claim_owned(bytes as u64, 1) {
                Ok(allocation) => {
                    *stages
                        .get_mut(stage)
                        .ok_or(InvalidPolicy("invalid binding stage"))? =
                        Some((allocation, bytes as u64));
                    stage += 1;
                    Ok(())
                }
                Err(error) => {
                    admission_error = Some(error);
                    Err(InvalidPolicy("binding storage was not admitted"))
                }
            },
        );
        let charged = accounting.charge_items((before - remaining) as u64);
        drop(volume);
        accounting.release_owned(&mut volume_hold, 36)?;
        let result = charged.and_then(|()| {
            if let Some(error) = admission_error {
                return Err(error);
            }
            compiled.map_err(|_| FsError::InvalidAccessPolicy)
        });
        let mut retained = result.as_ref().map_or(0, PathTrie::logical_owned_bytes);
        let mut custody = accounting.claim_owned(0, 0)?;
        for (mut allocation, requested) in stages.into_iter().flatten() {
            let actual = retained.min(requested);
            accounting.reconcile_owned(&mut allocation, requested, actual)?;
            retained -= actual;
            custody.absorb(&mut allocation)?;
        }
        if retained != 0 {
            return Err(FsError::InvalidAccessPolicy);
        }
        let view = result?.with_policy_revision(self.generation_id(), revision);
        let view = match binding.generation {
            Some(selected) => view.with_data_generation(selected),
            None => view,
        };
        Ok((view, custody))
    }
    async fn access_generation_facts(
        &self,
        binding: &AccessBinding<'_>,
        policy: &crate::access::CompiledPolicy,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(u64, u64), FsError> {
        let selected = binding.generation.unwrap_or(self.generation_id());
        let grants = binding
            .capability
            .layers
            .iter()
            .map(Vec::len)
            .sum::<usize>();
        accounting.charge_items(1 + grants as u64)?;
        let needs_ordinal = policy.needs_ordinal()
            || binding
                .capability
                .layers
                .iter()
                .flatten()
                .any(|grant| matches!(grant.resource.generations, Generations::Range(_)));
        Ok(if needs_ordinal {
            let (generation, head) = self
                .generation_position_accounted(selected, accounting, cancellation)
                .await?;
            (generation.get(), head.get())
        } else {
            (u64::from(selected == self.generation_id()), 1)
        })
    }
}
