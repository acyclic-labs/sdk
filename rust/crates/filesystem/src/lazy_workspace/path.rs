use super::*;


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    fn canonical_path(&self, path: &str) -> Result<String, LazyWorkspaceError> {
        PortablePath::parse(path, self.workspace.limits())
            .map(|path| path.as_str().to_owned())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))
    }

    pub(crate) fn namespace_path(&self, path: &str) -> Result<NamespacePath, LazyWorkspaceError> {
        let portable = PortablePath::parse(path, self.workspace.limits())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
        NamespacePath::from_portable_in_profile(
            &portable,
            self.workspace.profile(),
            self.workspace.limits(),
        )
        .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))
    }

}
