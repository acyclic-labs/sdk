use super::*;
impl Reducer {
    pub(crate) fn set_resident_terminal_effect_limit(&mut self, maximum: usize) -> Result<()> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "terminal effect cache limit must be positive".into(),
            ));
        }
        if maximum == usize::MAX {
            self.resident_terminal_effect_limit = maximum;
            return Ok(());
        }
        let protected = self
            .projection
            .effects
            .iter()
            .filter(|(id, effect)| {
                settled_effect(&effect.status)
                    && !self.projection.indexed_terminal_effects.contains(id)
            })
            .count();
        if protected > maximum {
            return Err(Error::Unsupported(
                "unindexed terminal effects exceed cache allowance".into(),
            ));
        }
        self.resident_terminal_effect_limit = maximum;
        self.trim_terminal_effects(protected);
        Ok(())
    }

    pub(crate) fn mark_indexed_terminal_effect_with_work(
        &mut self,
        event: &Event,
        _work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let EventPayload::EffectResolved { observation } = &event.payload else {
            return Ok(());
        };
        if !settled_effect(&observation.status) {
            return Ok(());
        }
        let state = self
            .projection
            .effects
            .get(&observation.effect_id)
            .ok_or_else(|| Error::Storage("indexed terminal effect is absent".into()))?;
        if state.status != observation.status {
            return Err(Error::Storage(
                "indexed terminal effect differs from admitted result".into(),
            ));
        }
        if self.projection.indexed_terminal_effects.back() != Some(&observation.effect_id) {
            self.projection
                .indexed_terminal_effects
                .push_back(observation.effect_id);
        }
        if self.resident_terminal_effect_limit == usize::MAX {
            return Ok(());
        }
        self.set_resident_terminal_effect_limit(self.resident_terminal_effect_limit)
    }

    pub(crate) fn enforce_terminal_effect_limit(&mut self) -> Result<()> {
        if self.resident_terminal_effect_limit == usize::MAX {
            return Ok(());
        }
        self.set_resident_terminal_effect_limit(self.resident_terminal_effect_limit)
    }

    pub(in crate::core) fn trim_terminal_effects(&mut self, protected: usize) {
        let budget = self
            .resident_terminal_effect_limit
            .saturating_sub(protected);
        while self.projection.indexed_terminal_effects.len() > budget {
            if let Some(effect) = self.projection.indexed_terminal_effects.pop_front() {
                self.projection.effects.remove(&effect);
                self.projection.effects_archived = true;
            }
        }
    }

    /// Number of resident active and cached terminal effect projections.
    #[must_use]
    pub fn resident_effect_count(&self) -> usize {
        self.projection.effects.len()
    }

    /// Cached known successes/failures; indeterminate effects remain active.
    #[must_use]
    pub fn resident_terminal_effect_count(&self) -> usize {
        self.projection
            .effects
            .values()
            .filter(|effect| settled_effect(&effect.status))
            .count()
    }

    /// Reconstructs one read-only effect using the same authorization and payload reducer as canonical replay.
    pub(crate) fn project_effect_with_retained_quota(
        authority: Authority,
        verifier: AuthorityVerifier,
        effect_id: EffectId,
        events: &[Event],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(EffectState, u64)> {
        let metadata_owned = if work.is_some() {
            (authority.id.capacity() as u64)
                .checked_add(verifier.logical_owned_bytes()?)
                .ok_or_else(|| Error::Invalid("effect metadata custody overflows".into()))?
        } else {
            0
        };
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, metadata_owned);
        let mut projection = Self::new(authority, verifier, SchemaRegistry::default());
        let mut previous = 0;
        for event in events {
            if event.revision <= previous || event_effect_id(&event.payload) != Some(effect_id) {
                return Err(Error::Storage(
                    "effect projection has invalid identity or order".into(),
                ));
            }
            projection
                .authority_verifier
                .verify_event(event, quota.work())?;
            projection.authorize(
                (&event.scope.id, &event.scope.capabilities),
                event.scope.agent(),
                event.operation_id,
                event.payload.admission(),
                None,
                quota.work(),
            )?;
            validate_causal_parent_with_work(
                &projection.authority,
                event.revision - 1,
                event.causal_parent.as_ref(),
                quota.work(),
            )?;
            if quota
                .work()
                .as_deref()
                .is_some_and(|io| !copying::is_native(io))
            {
                let owned = event_owned_bytes(event)?;
                let next_retained = quota
                    .retained()
                    .checked_add(owned)
                    .ok_or_else(|| Error::Invalid("effect projected custody overflows".into()))?;
                quota.replace(next_retained)?;
            }
            projection.apply_payload_with_work(&event.payload, event.revision, quota.work())?;
            previous = event.revision;
        }
        let effect = projection
            .projection
            .effects
            .remove(&effect_id)
            .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
        drop(projection);
        quota.relinquish(metadata_owned)?;
        let retained = quota.retained();
        quota.transfer();
        Ok((effect, retained))
    }

    /// Returns the current resident effect status.
    #[must_use]
    pub fn effect(&self, effect_id: EffectId) -> Option<&EffectState> {
        self.projection.effects.get(&effect_id)
    }
}
