//! Validate the resulting catalog before either mutation entry point commits.
use super::*;

pub(super) enum Changes {
    One(String, ProviderSpec),
    Many(BTreeMap<String, ProviderSpec>),
}

impl Changes {
    pub(super) fn empty() -> Self {
        Self::Many(BTreeMap::new())
    }
    fn entries(&self) -> impl Iterator<Item = (&String, &ProviderSpec)> {
        let (one, many): (_, Option<&BTreeMap<String, ProviderSpec>>) = match self {
            Self::One(id, spec) => (Some((id, spec)), None),
            Self::Many(providers) => (None, Some(providers)),
        };
        one.into_iter()
            .chain(many.into_iter().flat_map(BTreeMap::iter))
    }

    fn get(&self, id: &str) -> Option<&ProviderSpec> {
        match self {
            Self::One(provider, spec) => (provider == id).then_some(spec),
            Self::Many(providers) => providers.get(id),
        }
    }

    fn into_entries(self) -> impl Iterator<Item = (String, ProviderSpec)> {
        let (one, many): (_, Option<BTreeMap<String, ProviderSpec>>) = match self {
            Self::One(id, spec) => (Some((id, spec)), None),
            Self::Many(providers) => (None, Some(providers)),
        };
        one.into_iter()
            .chain(many.into_iter().flat_map(BTreeMap::into_iter))
    }
}

impl Registry {
    fn validate_alloys(
        &self,
        changes: &Changes,
        alloys: &BTreeMap<String, crate::AlloySpec>,
    ) -> Result<()> {
        // Validate the resulting graph before publishing providers, policies or a default.
        for (name, spec) in self
            .alloys
            .iter()
            .filter(|(name, _)| !alloys.contains_key(*name))
            .chain(alloys.iter())
        {
            if name.trim().is_empty()
                || name.contains('/')
                || name == "default_model"
                || name == "alloys"
            {
                return Err(invalid("alloy name must be a nonempty selection name"));
            }
            spec.validate()?;
            for member in &spec.models {
                let (provider, model) = member
                    .split_once('/')
                    .ok_or_else(|| invalid("alloy member requires provider/model"))?;
                if changes
                    .get(provider)
                    .or_else(|| self.providers.get(provider).map(Arc::as_ref))
                    .is_none_or(|entry| !entry.models.contains_key(model))
                {
                    return Err(invalid("alloy member is absent from the resulting catalog"));
                }
            }
        }
        Ok(())
    }

    pub(super) fn update_catalog(
        &mut self,
        changes: Changes,
        default: Option<String>,
        alloys: BTreeMap<String, crate::AlloySpec>,
    ) -> Result<()> {
        for (provider, spec) in changes.entries() {
            validate_spec(provider, spec)?;
        }
        self.validate_alloys(&changes, &alloys)?;
        let retained = self.default_model.as_ref();
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let retained = if tests::SKIP_RETAINED_DEFAULT.with(std::cell::Cell::get) {
            None
        } else {
            retained
        };
        // Admission observes only the default's provider, into a new generation.
        let mut observed = selection::Observations::default();
        let validation: Result<()> = (|| {
            if let Some(default) = default.as_ref().or(retained) {
                let policy = alloys.get(default).or_else(|| self.alloys.get(default));
                let members = policy.map_or_else(
                    || std::slice::from_ref(default),
                    |policy| policy.models.as_slice(),
                );
                for member in members {
                    let (provider, name) = member.split_once('/').ok_or_else(|| {
                        invalid("catalog default_model requires provider/model or a named alloy")
                    })?;
                    let entry = changes
                        .get(provider)
                        .or_else(|| self.providers.get(provider).map(Arc::as_ref));
                    let entry = entry
                        .filter(|entry| entry.models.contains_key(name))
                        .ok_or_else(|| {
                            invalid("catalog default_model is absent from its models")
                        })?;
                    let counted = selection::counts_input(
                        entry,
                        name,
                        self.observe(&mut observed, provider)?,
                    );
                    #[cfg(all(test, not(target_arch = "wasm32")))]
                    let counted = if tests::SKIP_DEFAULT_COUNTER.with(std::cell::Cell::get) {
                        Ok(())
                    } else {
                        counted
                    };
                    counted?;
                }
            }
            Ok(())
        })();
        self.selection
            .publish(|current| {
                if validation.is_ok() {
                    *current = observed;
                } else {
                    current.adopt(observed);
                }
            })
            .map_err(|()| invalid("credential selection lock poisoned"))?;
        validation?;
        self.providers.extend(
            changes
                .into_entries()
                .map(|(id, spec)| (id, Arc::new(spec))),
        );
        self.alloys.extend(alloys);
        if let Some(default) = default {
            self.default_model = Some(default);
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
pub(super) mod tests;
