//! reducer effects semantics.
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
            .effects
            .iter()
            .filter(|(id, effect)| settled_effect(&effect.status) && !self.indexed_terminal_effects.contains(id))
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
        &mut self, event: &Event,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.native_custody.require_work(work, "native terminal indexing requires explicit native work")?;
        let EventPayload::EffectResolved { observation } = &event.payload else {
            return Ok(());
        };
        if !settled_effect(&observation.status) {
            return Ok(());
        }
        let state = self
            .effects
            .get(&observation.effect_id)
            .ok_or_else(|| Error::Storage("indexed terminal effect is absent".into()))?;
        if state.status != observation.status {
            return Err(Error::Storage(
                "indexed terminal effect differs from admitted result".into(),
            ));
        }
        if self.indexed_terminal_effects.back() != Some(&observation.effect_id) {
            Self::reserve_deque_buffer(&mut self.native_custody,
                NativeComponentKey::TerminalEffectBuffer, &mut self.indexed_terminal_effects, work)?;
            self.indexed_terminal_effects
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
        while self.indexed_terminal_effects.len() > budget {
            if let Some(effect) = self.indexed_terminal_effects.pop_front() {
                self.effects.remove(&effect);
                self.native_custody.remove(&NativeComponentKey::EffectAction(effect));
                self.native_custody.remove(&NativeComponentKey::EffectStatus(effect));
                self.native_custody.remove(&NativeComponentKey::EffectAttempts(effect));
                self.effects_archived = true;
            }
        }
    }

    /// Number of resident active and cached terminal effect projections.
    #[must_use]
    pub fn resident_effect_count(&self) -> usize {
        self.effects.len()
    }

    /// Cached known successes/failures; indeterminate effects remain active.
    #[must_use]
    pub fn resident_terminal_effect_count(&self) -> usize {
        self.effects
            .values()
            .filter(|effect| settled_effect(&effect.status))
            .count()
    }

    /// Reconstructs one read-only effect using the same authorization and payload reducer as canonical replay.
    pub(crate) fn project_effect_with_retained_quota(
        authority: Authority, verifier: AuthorityVerifier, effect_id: EffectId,
        events: &[Event], work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(EffectState, u64)> {
        let metadata_owned = if work.is_some() {
            (authority.id.capacity() as u64).checked_add(verifier.logical_owned_bytes()?)
                .ok_or_else(|| Error::Invalid("effect metadata custody overflows".into()))?
        } else { 0 };
        let mut retained = 0_u64;
        let mut projection = Self::new(authority, verifier, SchemaRegistry::default());
        let mut previous = 0;
        let result = (|| {
        for event in events {
            if event.revision <= previous || event_effect_id(&event.payload) != Some(effect_id) {
                return Err(Error::Storage(
                    "effect projection has invalid identity or order".into(),
                ));
            }
            projection.authority_verifier.verify_event(event, work)?;
            projection.authorize(
                (&event.scope.id, &event.scope.capabilities),
                event.scope.agent(),
                event.operation_id,
                event.payload.admission(),
                None,
                work,
            )?;
            validate_causal_parent_with_work(
                &projection.authority,
                event.revision - 1,
                event.causal_parent.as_ref(), work,
            )?;
            if let Some(work) = work.as_deref_mut().filter(|io| !copying::is_native(&**io)) {
                let owned = inventory::source_inventory(event)?;
                let next_retained = retained.checked_add(owned)
                    .ok_or_else(|| Error::Invalid("effect projected custody overflows".into()))?;
                work.admit_owned(owned)?;
                work.retain_owned(owned)?;
                retained = next_retained;
            }
            projection.apply_payload_with_work(&event.payload, event.revision, work)?;
            previous = event.revision;
        }
        let effect = projection.effects.remove(&effect_id)
            .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some()) {
            retained = projection.native_custody.transfer_effect(effect_id, io)?;
        }
        Ok(effect)
        })();
        drop(projection);
        let released_metadata = match work.as_deref_mut() {
            Some(work) => work.release_owned(metadata_owned),
            None => Ok(()),
        };
        match result {
            Err(error) => {
                if let Some(work) = work.as_deref_mut() { let _ = work.release_owned(retained); }
                Err(error)
            }
            Ok(effect) => match released_metadata {
                Ok(()) => Ok((effect, retained)),
                Err(error) => {
                    drop(effect);
                    if let Some(work) = work.as_deref_mut() { let _ = work.release_owned(retained); }
                    Err(error)
                }
            },
        }
    }

    /// Returns the current resident effect status.
    #[must_use]
    pub fn effect(&self, effect_id: EffectId) -> Option<&EffectState> {
        self.effects.get(&effect_id)
    }

}

