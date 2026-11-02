use crate::Registry;
use acyclic_harness::{
    local::{LocalHarnessConfig, ModelResolver},
    preset::Preset,
};
use std::{path::PathBuf, sync::Arc};

impl Registry {
    /// Builds the shared GraphCoder preset over this registry's catalog and credentials.
    ///
    /// The local harness resolves the explicit selection, then native settings, then
    /// the catalog default when it opens. Approval models use the same resolver.
    /// Workspace settings, working directory and approval policy remain configurable
    /// through the returned original harness configuration.
    #[must_use]
    pub fn graphcoder(
        self,
        workspace: impl Into<PathBuf>,
        selection: Option<String>,
    ) -> acyclic_harness_core::Result<LocalHarnessConfig> {
        // Publish only the model contract. Endpoint headers, credential policy,
        // transport settings and secret bindings remain in the host registry.
        #[derive(serde::Serialize)]
        struct Catalog<'a> {
            version: u32,
            providers: std::collections::BTreeMap<
                &'a str,
                &'a std::collections::BTreeMap<String, crate::ModelSpec>,
            >,
        }
        let bytes = serde_json::to_vec(&Catalog {
            version: 1,
            providers: self
                .catalog()
                .iter()
                .map(|(name, spec)| (name.as_str(), &spec.models))
                .collect(),
        })
        .map_err(|_| crate::invalid("cannot encode public model catalog"))?;
        let models = ModelResolver::new(self.default_model().to_owned(), move |selection| {
            let provider = Arc::new(self.resolve(selection)?);
            Ok((provider.model().clone(), provider as _))
        });
        Ok(
            LocalHarnessConfig::resolving(workspace, Arc::new(models), selection)
                .system_file("sys/models/catalog.json", bytes.into())
                .preset(&Preset::graphcoder())
                .web_tools(),
        )
    }
}
