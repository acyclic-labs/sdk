//! Validate the resulting catalog before either mutation entry point commits.
use super::*;

pub(super) enum Changes {
    One(String, ProviderSpec),
    Many(BTreeMap<String, ProviderSpec>),
}

impl Changes {
    fn entries(&self) -> impl Iterator<Item = (&String, &ProviderSpec)> {
        let (one, many) = match self {
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
        let (one, many) = match self {
            Self::One(id, spec) => (Some((id, spec)), None),
            Self::Many(providers) => (None, Some(providers)),
        };
        one.into_iter()
            .chain(many.into_iter().flat_map(BTreeMap::into_iter))
    }
}

impl Registry {
    pub(super) fn update_catalog(
        &mut self,
        changes: Changes,
        default: Option<String>,
    ) -> Result<()> {
        for (provider, spec) in changes.entries() {
            validate_spec(provider, spec)?;
        }
        let retained = self.default_model.as_ref();
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let retained = if tests::SKIP_RETAINED_DEFAULT.with(std::cell::Cell::get) {
            None
        } else {
            retained
        };
        if let Some(default) = default.as_ref().or(retained) {
            let (provider, name) = default
                .split_once('/')
                .ok_or_else(|| invalid("catalog default_model requires provider/model"))?;
            let entry = changes
                .get(provider)
                .or_else(|| self.providers.get(provider).map(Arc::as_ref));
            if entry.is_none_or(|entry| !entry.models.contains_key(name)) {
                return Err(invalid("catalog default_model is absent from its models"));
            }
        }
        self.providers.extend(
            changes
                .into_entries()
                .map(|(id, spec)| (id, Arc::new(spec))),
        );
        if let Some(default) = default {
            self.default_model = Some(default);
        }
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
