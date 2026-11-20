mod tests {
    use super::super::*;
    use acyclic_harness_core::*;
    use acyclic_harness_workspace::checkout_tools::hooks::owner_check;
    use acyclic_harness_workspace::checkout_tools::hooks::*;
    use acyclic_harness_workspace::workspace::*;
    use acyclic_harness_workspace::*;

    #[tokio::test]
    async fn owner_checks_reject_stopped_commands_and_replaced_physical_roots() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let root = directory.path().join("project");
        std::fs::create_dir(&root).map_err(storage)?;
        let harness = LocalHarness::open(
            LocalHarnessConfig::native_host(&root)
                .workspace_mounting(WorkspaceMounting::Physical)
                .state_directory(directory.path().join("runtime"))
                .without_user_settings(),
        )
        .await?;
        let workspace = harness
            .workspace()
            .ok_or_else(|| Error::NotFound("physical workspace".into()))?;
        let control = ProcessControl::unbounded().map_err(storage)?;
        let check = owner_check(Arc::clone(workspace), control.clone());
        check().map_err(storage)?;
        control.stop();
        let Err(stopped) = check() else {
            return Err(Error::Invalid("stopped command retained authority".into()));
        };
        assert_eq!(stopped.kind(), std::io::ErrorKind::Interrupted);

        let check = owner_check(
            Arc::clone(workspace),
            ProcessControl::unbounded().map_err(storage)?,
        );
        std::fs::rename(&root, directory.path().join("retired")).map_err(storage)?;
        std::fs::create_dir(&root).map_err(storage)?;
        let Err(replaced) = check() else {
            return Err(Error::Invalid("replacement root retained authority".into()));
        };
        assert_eq!(replaced.kind(), std::io::ErrorKind::PermissionDenied);
        Ok(())
    }
}
