//! The same production-bound oracles run in unit tests and fuzz targets.
use super::{model_selection as selection, workspace};
mod model;
#[cfg(feature = "test-support")]
pub use model::exercise as check_model_selection;
#[path = "validation/workspace.rs"]
mod workspace_contract;
#[cfg(feature = "test-support")]
pub use workspace_contract::exercise as check_workspace;

#[cfg(test)]
proptest::proptest! {
    #[test]
    fn shared_workspace_contract(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..128)) {
        workspace_contract::exercise(&input);
    }

    #[test]
    fn shared_model_selection_contract(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..128)) {
        model::exercise(&input);
    }
}
