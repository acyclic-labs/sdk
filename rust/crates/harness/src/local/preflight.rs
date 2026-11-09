//! Independent native prerequisites, shared by local startup and host doctors.
use super::*;

impl LocalHarnessConfig {
    /// Collect native prerequisites even if model/settings admission will fail.
    /// Checks state separation and signing policy without creating directories,
    /// allocating temporary state, opening storage or acquiring a signing key.
    pub fn preflight(&self) -> Vec<Error> {
        let mut failures = self.native_preflight();
        failures.extend(self.state_refusals());
        failures
    }

    /// Native facilities shared with plugin doctor. Host plugins supply their
    /// owner's state directory separately, so their host admission checks only
    /// these facilities, without inventing a default state location.
    pub fn native_preflight(&self) -> Vec<Error> {
        let native = self.default_tools.iter().any(|name| name == "shell")
            || self
                .tools
                .iter()
                .any(|tool| tool.definition.name == "shell");
        self.native_refusals(native)
    }

    pub(super) fn state_refusals(&self) -> Vec<Error> {
        let mut failures = Vec::new();
        // Opening alone prepares lazy hermetic state. There is no selected
        // directory to inspect until then; doctor must not create one.
        if !self.hermetic || self.state_directory.is_some() {
            let state = self.state_location().and_then(|location| {
                let (config::StateLocation::Directory(path)
                | config::StateLocation::ProjectRoot(path)) = location;
                let workspace = match &self.project {
                    LocalProject::Workspace(path) => Some(path.as_path()),
                    LocalProject::Volume { .. } => None,
                };
                owner::planned_separated_directory(&path, workspace)
            });
            if let Err(error) = state {
                failures.push(Error::Invalid(format!("state directory: {error}; recovery: select a user data directory outside the checkout (XDG_DATA_HOME on Linux, LOCALAPPDATA on Windows)")));
            }
        }
        if !self.without_user_settings {
            failures.extend(
                acyclic_native_runtime::signing_key::SigningCustody::Configured
                    .validate()
                    .err()
                    .map(storage),
            );
        }
        failures
    }

    pub(super) fn native_refusals(&self, native: bool) -> Vec<Error> {
        let mut failures = Vec::new();
        #[cfg(any(test, feature = "native-execution"))]
        if native {
            if matches!(self.project, LocalProject::Workspace(_))
                && let Err(error) =
                    workspace::volumes::require_available(self.native_mount_probe.probe())
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
