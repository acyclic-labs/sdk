//! Independent native prerequisites, shared by local startup and host doctors.
use super::*;

impl LocalHarnessConfig {
    /// Collect native prerequisites even if model/settings admission will fail.
    /// Checks the selected state root with a disposable write, without opening
    /// an agent or acquiring its signing key.
    pub fn preflight(&self) -> Vec<Error> {
        let mut failures = self.native_preflight();
        let state = self.check_state_directory();
        if let Err(error) = state {
            failures.push(Error::Invalid(format!("state directory: {error}; recovery: select a writable user data directory outside the checkout (XDG_DATA_HOME on Linux, LOCALAPPDATA on Windows)")));
        }
        failures
    }

    /// Native facilities shared with plugin doctor. Host plugins supply their
    /// owner's state directory separately, so their host admission checks only
    /// these facilities, without inventing a default state location.
    pub fn native_preflight(&self) -> Vec<Error> {
        let native = self.model.is_none() && {
            #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
            {
                self.models.is_none()
            }
            #[cfg(not(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")))]
            {
                true
            }
        } || self.default_tools.iter().any(|name| name == "shell")
            || self
                .tools
                .iter()
                .any(|tool| tool.definition.name == "shell");
        self.native_refusals(native)
    }

    fn check_state_directory(&self) -> Result<()> {
        use std::io::Write as _;
        let location = self.state_location()?;
        let (config::StateLocation::Directory(path) | config::StateLocation::ProjectRoot(path)) =
            location;
        let workspace = match &self.project {
            LocalProject::Workspace(path) => Some(path.as_path()),
            LocalProject::Volume { .. } => None,
        };
        let path = owner::separated_directory(&path, workspace)?;
        std::fs::create_dir_all(&path).map_err(storage)?;
        let mut probe = tempfile::tempfile_in(path).map_err(storage)?;
        probe.write_all(b"preflight").map_err(storage)?;
        probe.sync_all().map_err(storage)
    }

    pub(super) fn native_refusals(&self, native: bool) -> Vec<Error> {
        let mut failures = Vec::new();
        #[cfg(any(test, feature = "native-execution"))]
        if native {
            if matches!(self.project, LocalProject::Workspace(_))
                && let Err(error) =
                    workspace::volumes::require_available(acyclic_fs::probe_native_mount())
            {
                failures.push(error);
            }
            let passive = self.default_tools.is_empty() && self.tools.is_empty();
            if !passive && let Err(error) = checkout_tools::shell::executable() {
                failures.push(Error::Invalid(format!(
                    "shell: {error}; recovery: install the native platform shell and add it to PATH"
                )));
            }
        }
        #[cfg(not(any(test, feature = "native-execution")))]
        if native && matches!(self.project, LocalProject::Workspace(_)) {
            failures.push(Error::Unsupported("checkout workspaces require native-execution; recovery: install a native-execution build".into()));
        }
        if let LocalProject::Workspace(path) = &self.project
            && !path.is_dir()
        {
            failures.push(Error::Invalid(format!("local workspace {} is not a directory; recovery: select an existing --workspace directory", path.display())));
        }
        failures
    }
}
