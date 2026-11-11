//! The same production-bound oracles run in unit tests and fuzz targets.
use super::{model_selection as selection, workspace};
#[cfg(not(target_arch = "wasm32"))]
use crate as harness;

mod model;
pub use model::exercise as check_model_selection;
#[cfg(not(target_arch = "wasm32"))]
mod settings;
#[cfg(not(target_arch = "wasm32"))]
pub use settings::exercise as check_settings;
#[path = "validation/workspace.rs"]
mod workspace_contract;
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

#[cfg(all(test, not(target_arch = "wasm32")))]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn shared_settings_selection_contract(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64)) {
        settings::exercise(&input);
    }
}
