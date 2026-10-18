//! Validate the resulting catalog before either mutation entry point commits.
use super::*;

pub(super) enum Changes {
    One(String, ProviderSpec),
    #[cfg(not(target_arch = "wasm32"))]
    Many(BTreeMap<String, ProviderSpec>),
}

impl Changes {
    fn entries(&self) -> impl Iterator<Item = (&String, &ProviderSpec)> {
        let (one, many): (_, Option<&BTreeMap<String, ProviderSpec>>) = match self {
            Self::One(id, spec) => (Some((id, spec)), None),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Many(providers) => (None, Some(providers)),
        };
        one.into_iter()
            .chain(many.into_iter().flat_map(BTreeMap::iter))
    }

    fn get(&self, id: &str) -> Option<&ProviderSpec> {
        match self {
            Self::One(provider, spec) => (provider == id).then_some(spec),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Many(providers) => providers.get(id),
        }
    }

    fn into_entries(self) -> impl Iterator<Item = (String, ProviderSpec)> {
        let (one, many): (_, Option<BTreeMap<String, ProviderSpec>>) = match self {
            Self::One(id, spec) => (Some((id, spec)), None),
            #[cfg(not(target_arch = "wasm32"))]
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
            let entry = entry
                .filter(|entry| entry.models.contains_key(name))
                .ok_or_else(|| invalid("catalog default_model is absent from its models"))?;
            let counted = counts_input(entry);
            #[cfg(all(test, not(target_arch = "wasm32")))]
            let counted = counted || tests::SKIP_DEFAULT_COUNTER.with(std::cell::Cell::get);
            if !counted {
                return Err(invalid(
                    "catalog default_model requires a verified input token counter",
                ));
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
pub(super) mod tests;
