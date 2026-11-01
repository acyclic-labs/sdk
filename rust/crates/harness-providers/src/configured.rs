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
    ) -> LocalHarnessConfig {
        let models = ModelResolver::new(self.default_model().to_owned(), move |selection| {
            let provider = Arc::new(self.resolve(selection)?);
            Ok((provider.model().clone(), provider as _))
        });
        LocalHarnessConfig::resolving(workspace, Arc::new(models), selection)
            .preset(&Preset::graphcoder())
    }
}
