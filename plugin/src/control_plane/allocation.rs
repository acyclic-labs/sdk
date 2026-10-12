//! Selected root allocation state.

use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RootBinding {
    pub(crate) root_id: [u8; 16],
    pub(crate) path: PathBuf,
    pub(crate) allocation: RootAllocation,
}

/// Selected root intent exists before allocation. Only Created carries usable
/// workspace/source references; neither variant grants authority by itself.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "creation",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum RootAllocation {
    Preparing,
    Created(RootCreation),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RootCreation {
    pub(crate) repository_workspace_id: [u8; 16],
    pub(crate) source_identity: [u8; 16],
    /// Current retained watcher observation; zero is valid and this advances
    /// during ordinary recovery/reconciliation without changing allocation.
    pub(crate) source_epoch: u64,
    pub(crate) native_root_identity: [u8; 16],
}

impl RootCreation {
    pub(crate) fn validate(&self) -> Result<(), String> {
            if [
                self.repository_workspace_id,
                self.source_identity,
                self.native_root_identity,
            ]
            .contains(&[0; 16])
            {
                return Err("physical root creation has an empty identity".to_owned());
            }
            Ok(())
        }

}

impl RootBinding {
    pub(crate) fn created(&self) -> Result<&RootCreation, String> {
            match &self.allocation {
                RootAllocation::Created(creation) => Ok(creation),
                RootAllocation::Preparing => {
                    Err("physical root allocation is still preparing".to_owned())
                }
            }
        }

    pub(crate) fn created_mut(&mut self) -> Result<&mut RootCreation, String> {
            match &mut self.allocation {
                RootAllocation::Created(creation) => Ok(creation),
                RootAllocation::Preparing => {
                    Err("physical root allocation is still preparing".to_owned())
                }
            }
        }

}
